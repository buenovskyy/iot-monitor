import time
import json
import logging
from datetime import datetime
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask import request

logger = logging.getLogger('websocket')

socketio = None

#stats
ws_stats = {
    'conexoes_ativas': 0,
    'total_conexoes': 0,
    'total_mensagens': 0,
    'total_leituras_ws': 0,
    'total_alertas_ws': 0,
}


def init_socketio(app):
    global socketio
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
    registrar_eventos(socketio)
    logger.info("WebSocket (SocketIO) inicializado")
    return socketio


def registrar_eventos(sio):
    @sio.on('connect')
    def handle_connect():
        ws_stats['conexoes_ativas'] += 1
        ws_stats['total_conexoes'] += 1
        client_id = request.sid
        logger.info(
            f"WebSocket CONECTADO: {client_id} | "
            f"Ativas: {ws_stats['conexoes_ativas']}"
        )
        emit('conexao_confirmada', {
            'status': 'conectado',
            'client_id': client_id,
            'timestamp': datetime.now().isoformat(),
            'mensagem': 'Conexão WebSocket estabelecida com sucesso'
        })

    @sio.on('disconnect')
    def handle_disconnect():
        ws_stats['conexoes_ativas'] = max(0, ws_stats['conexoes_ativas'] - 1)
        logger.info(
            f"WebSocket DESCONECTADO: {request.sid} | "
            f"Ativas: {ws_stats['conexoes_ativas']}"
        )

    @sio.on('inscrever')
    def handle_inscrever(data):
        canal = data.get('canal', 'leituras')
        join_room(canal)
        logger.info(f"Cliente {request.sid} inscrito no canal: {canal}")
        emit('inscricao_confirmada', {
            'canal': canal,
            'status': 'inscrito',
            'timestamp': datetime.now().isoformat()
        })

    @sio.on('desinscrever')
    def handle_desinscrever(data):
        canal = data.get('canal', 'leituras')
        leave_room(canal)
        logger.info(f"Cliente {request.sid} desinscrito do canal: {canal}")
        emit('desinscricao_confirmada', {'canal': canal, 'status': 'desinscrito'})

    #envio das leituras
    @sio.on('enviar_leitura')
    def handle_leitura(data):
        import sqlite3
        ws_stats['total_mensagens'] += 1
        ws_stats['total_leituras_ws'] += 1
        t_recebido = time.time()

        #auth
        token = data.get('token', '')
        db = sqlite3.connect('iot_monitor.db')
        db.row_factory = sqlite3.Row
        usuario = db.execute(
            'SELECT id, username FROM usuarios WHERE token = ?', (token,)
        ).fetchone()

        if not usuario:
            emit('erro', {'mensagem': 'Token inválido', 'tipo': 'auth'})
            db.close()
            return

        sensor_id = data.get('sensor_id')
        valor = data.get('valor')
        unidade = data.get('unidade', '')

        #sensor
        sensor = db.execute(
            'SELECT * FROM sensores WHERE id = ? AND usuario_id = ?',
            (sensor_id, usuario['id'])
        ).fetchone()

        if not sensor:
            emit('erro', {'mensagem': 'Sensor não encontrado', 'tipo': 'sensor'})
            db.close()
            return

        #leitura salva
        db.execute(
            'INSERT INTO leituras (sensor_id, valor, unidade, protocolo) VALUES (?, ?, ?, ?)',
            (sensor_id, valor, unidade, 'WebSocket')
        )
        db.commit()

        #alerta
        alerta = _verificar_alerta_ws(db, sensor_id, sensor['tipo'], valor)

        #rtt estimado
        ts_cliente = data.get('timestamp_envio', 0)
        rtt_ms = (t_recebido - ts_cliente) * 1000 if ts_cliente > 0 else 0

        #faz o log
        db.execute('''
            INSERT INTO logs_rede (origem, destino, protocolo, tamanho_bytes, rtt_ms, throughput_kbps)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (request.sid, 'WebSocket', 'WebSocket', len(json.dumps(data)), round(rtt_ms, 2), 0))
        db.commit()
        db.close()

        #payload
        leitura_payload = {
            'sensor_id': sensor_id,
            'sensor_nome': data.get('sensor_nome', sensor['nome']),
            'tipo': sensor['tipo'],
            'valor': valor,
            'unidade': unidade,
            'protocolo': 'WebSocket',
            'timestamp': datetime.now().isoformat(),
            'rtt_ms': round(rtt_ms, 2)
        }

        #confirmacao para o remetente
        emit('leitura_confirmada', {
            'status': 'ok',
            'leitura': leitura_payload,
            'seq': data.get('seq', 0)
        })

        sio.emit('nova_leitura', leitura_payload, room='leituras')
        sio.emit('nova_leitura', leitura_payload, room=f'sensor_{sensor_id}')

        if alerta:
            ws_stats['total_alertas_ws'] += 1
            sio.emit('novo_alerta', alerta, room='alertas')
            sio.emit('novo_alerta', alerta, room='leituras')

        logger.info(
            f"WS LEITURA: {usuario['username']} | sensor={sensor_id} "
            f"valor={valor}{unidade} | RTT={rtt_ms:.1f}ms"
        )

    #websocket
    @sio.on('ws_ping')
    def handle_ping(data):
        ws_stats['total_mensagens'] += 1
        emit('ws_pong', {
            'timestamp_servidor': time.time(),
            'timestamp_cliente': data.get('timestamp', 0),
            'seq': data.get('seq', 0)
        })

    #pedir stats
    @sio.on('solicitar_stats')
    def handle_stats(data=None):
        emit('ws_stats', {
            **ws_stats,
            'timestamp': datetime.now().isoformat()
        })

    #chat
    @sio.on('chat_mensagem')
    def handle_chat(data):
        ws_stats['total_mensagens'] += 1
        payload = {
            'usuario': data.get('usuario', 'Anônimo'),
            'mensagem': data.get('mensagem', ''),
            'timestamp': datetime.now().isoformat()
        }
        sio.emit('chat_nova_mensagem', payload, room='leituras')
        logger.info(f"WS CHAT: {payload['usuario']}: {payload['mensagem'][:50]}")


def _verificar_alerta_ws(db, sensor_id, tipo, valor):
    LIMITES = {
        'temperatura': {'min': 10, 'max': 40, 'unidade': '°C'},
        'umidade':     {'min': 20, 'max': 80, 'unidade': '%'},
        'pressao':     {'min': 950, 'max': 1050, 'unidade': 'hPa'},
    }
    limites = LIMITES.get(tipo)
    if not limites:
        return None

    if valor < limites['min'] or valor > limites['max']:
        msg = f"ALERTA WS: {tipo} fora do limite! Valor: {valor}{limites['unidade']}"
        nivel = 'critical'
    elif valor > limites['max'] * 0.9 or valor < limites['min'] * 1.1:
        msg = f"AVISO WS: {tipo} próximo do limite. Valor: {valor}{limites['unidade']}"
        nivel = 'warning'
    else:
        return None

    db.execute(
        'INSERT INTO alertas (sensor_id, mensagem, nivel) VALUES (?, ?, ?)',
        (sensor_id, msg, nivel)
    )
    db.commit()
    logger.warning(msg)
    return {
        'sensor_id': sensor_id,
        'mensagem': msg,
        'nivel': nivel,
        'timestamp': datetime.now().isoformat()
    }


#notificar outros modulos
def notificar_nova_leitura(sensor_id, sensor_nome, tipo, valor, unidade, protocolo):
    if socketio is None:
        return

    payload = {
        'sensor_id': sensor_id,
        'sensor_nome': sensor_nome,
        'tipo': tipo,
        'valor': valor,
        'unidade': unidade,
        'protocolo': protocolo,
        'timestamp': datetime.now().isoformat()
    }
    socketio.emit('nova_leitura', payload, room='leituras')
    socketio.emit('nova_leitura', payload, room=f'sensor_{sensor_id}')


def notificar_alerta(sensor_id, mensagem, nivel):
    if socketio is None:
        return

    payload = {
        'sensor_id': sensor_id,
        'mensagem': mensagem,
        'nivel': nivel,
        'timestamp': datetime.now().isoformat()
    }
    socketio.emit('novo_alerta', payload, room='alertas')
    socketio.emit('novo_alerta', payload, room='leituras')


def get_ws_stats():
    return ws_stats.copy()