from server import app, init_db
from ws_module import init_socketio
from udp_server import iniciar_udp_em_thread

init_db()
socketio = init_socketio(app)
iniciar_udp_em_thread()

print("=" * 60)
print("  IoT Monitor Server v1.0")
print("  HTTP:      http://0.0.0.0:5000")
print("  WebSocket: ws://0.0.0.0:5000/socket.io")
print("  UDP:       0.0.0.0:5006")
print("=" * 60)

socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
