from collections import defaultdict

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")


class ChatRooms:
    """Tracks WebSocket connections in each room (in this server process)."""

    def __init__(self):
        self.rooms: dict[str, dict[WebSocket, str]] = defaultdict(dict)
        self.typing: dict[str, set[str]] = defaultdict(set)

    async def join(self, websocket: WebSocket, room: str, username: str):
        await websocket.accept()
        self.rooms[room][websocket] = username
        await self.broadcast(room, {"type": "system", "text": f"{username} joined the room."})

    def leave(self, websocket: WebSocket, room: str) -> str | None:
        username = self.rooms.get(room, {}).pop(websocket, None)
        if username:
            self.typing.get(room, set()).discard(username)
        if room in self.rooms and not self.rooms[room]:
            del self.rooms[room]
            self.typing.pop(room, None)
        return username

    async def set_typing(self, room: str, username: str, is_typing: bool):
        typing_users = self.typing[room]
        if is_typing:
            typing_users.add(username)
        else:
            typing_users.discard(username)
        if not typing_users:
            self.typing.pop(room, None)
        await self.broadcast(
            room,
            {"type": "typing", "usernames": sorted(self.typing.get(room, set()))},
        )

    async def broadcast(self, room: str, message: dict):
        # Copy the keys because disconnected sockets may be removed as we iterate.
        for connection in list(self.rooms.get(room, {})):
            try:
                await connection.send_json(message)
            except (RuntimeError, OSError):
                self.leave(connection, room)


chat_rooms = ChatRooms()

@app.get("/health")
def health_check():
    return "OK"


@app.get("/")
def read_index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="websockets_example.html",
        context={},
    )


@app.websocket("/ws/{room}/{username}")
async def chat(websocket: WebSocket, room: str, username: str):
    # Keep identifiers simple and bounded for this learning example.
    room = room.strip()[:40]
    username = username.strip()[:24]
    if not room or not username:
        await websocket.close(code=1008, reason="Room and name are required")
        return

    await chat_rooms.join(websocket, room, username)
    try:
        while True:
            payload = await websocket.receive_json()
            if not isinstance(payload, dict):
                continue

            if payload.get("type") == "typing":
                await chat_rooms.set_typing(room, username, bool(payload.get("is_typing")))
                continue

            if payload.get("type") != "message":
                continue
            text = str(payload.get("text", "")).strip()
            if text:
                await chat_rooms.set_typing(room, username, False)
                await chat_rooms.broadcast(
                    room,
                    {"type": "message", "username": username, "text": text[:1000]},
                )
    except WebSocketDisconnect:
        departed = chat_rooms.leave(websocket, room)
        if departed:
            await chat_rooms.broadcast(
                room,
                {"type": "system", "text": f"{departed} left the room."},
            )
