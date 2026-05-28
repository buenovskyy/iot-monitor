import socket
import json
import sqlite3
import threading
import logging
import time
from datetime import datetime

DATABASE = 'iot_monitor.db'
UDP_HOST = '0.0.0.0'
UDP_PORT = 5006
BUFFER_SIZE = 4096

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('udp_server')

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def autenticar_token(token):
    db = get_db()
    usuario = db.execute(
        'SELECT id, username FROM usuarios WHERE token = ?', (token,)
    ).fetchone()
    db.close()
    if usuario:
        return usuario['id'], usuario['username']
    return None, None


def verificar_sensor(sensor_id, usuario_id):
    db = get_db()
    sensor = db.execute(
        'SELECT * FROM sensores WHERE id = ? AND usuario_id = ?',
        (sensor_id, usuario_id)
    ).fetchone()
    db.close()
    return dict(sensor) if sensor else None


def salvar_leitura_udp(sensor_id, valor, unidade):
    db = get_db()
    db.execute(
        'INSERT INTO leituras (sensor_id, valor, unidade, protocolo) VALUES (?, ?, ?, ?)',
        (sensor_id, valor, unidade, 'UDP')
    )
    db.commit()
    db.close()


def verificar_alerta(sensor_id, tipo, valor):
    LIMITES = {
        'temperatura': {'min': 10, 'max': 40, 'unidade': '°C'},
        'umidade':     {'min': 20, 'max': 80, 'unidade': '%'},
        'pressao':     {'min': 950, 'max': 1050, 'unidade': 'hPa'},
    }
    limites = LIMITES.get(tipo)
    if not limites:
        return None

    if valor < limites['min'] or valor > limites['max']:
        msg = f"ALERTA UDP: {tipo} fora do limite! Valor: {valor}{limites['unidade']}"
        nivel = 'critical'
    elif valor > limites['max'] * 0.9 or valor < limites['min'] * 1.1:
        msg = f"AVISO UDP: {tipo} próximo do limite. Valor: {valor}{limites['unidade']}"
        nivel = 'warning'
    else:
        return None

    db = get_db()
    db.execute(
        'INSERT INTO alertas (sensor_id, mensagem, nivel) VALUES (?, ?, ?)',
        (sensor_id, msg, nivel)
    )
    db.commit()
    db.close()
    logger.warning(msg)
    return msg


def registrar_log_rede(origem, tamanho_req, tamanho_resp, rtt_ms):
    throughput = 0
    if rtt_ms > 0:
        throughput = ((tamanho_req + tamanho_resp) * 8) / (rtt_ms / 1000) / 1000

    db = get_db()
    db.execute('''
        INSERT INTO logs_rede (origem, destino, protocolo, tamanho_bytes, rtt_ms, throughput_kbps)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (origem, f'UDP:{UDP_PORT}', 'UDP', tamanho_req, round(rtt_ms, 2), round(throughput, 2)))
    db.commit()
    db.close()


def processar_mensagem(dados_raw, endereco):
    t_recebido = time.time()

    try:
        dados = json.loads(dados_raw.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return json.dumps({
            'status': 'erro',
            'mensagem': 'JSON inválido',
            'timestamp_servidor': t_recebido
        }).encode()

    tipo = dados.get('tipo', 'leitura')

    if tipo == 'ping':
        resposta = {
            'status': 'ok',
            'tipo': 'pong',
            'timestamp_servidor': t_recebido,
            'timestamp_cliente': dados.get('timestamp_envio', 0),
            'seq_recebido': dados.get('seq', 0)
        }
        resp_bytes = json.dumps(resposta).encode()
        logger.info(f"UDP PING de {endereco[0]}:{endereco[1]}")
        return resp_bytes

    token = dados.get('token', '')
    usuario_id, username = autenticar_token(token)
    if not usuario_id:
        return json.dumps({
            'status': 'erro',
            'mensagem': 'Token inválido',
            'timestamp_servidor': t_recebido
        }).encode()

    if tipo == 'leitura':
        sensor_id = dados.get('sensor_id')
        valor = dados.get('valor')
        unidade = dados.get('unidade', '')

        if sensor_id is None or valor is None:
            return json.dumps({
                'status': 'erro',
                'mensagem': 'sensor_id e valor são obrigatórios'
            }).encode()

        sensor = verificar_sensor(sensor_id, usuario_id)
        if not sensor:
            return json.dumps({
                'status': 'erro',
                'mensagem': 'Sensor não encontrado'
            }).encode()

        salvar_leitura_udp(sensor_id, valor, unidade)
        alerta = verificar_alerta(sensor_id, sensor['tipo'], valor)

        try:
            from ws_module import notificar_nova_leitura, notificar_alerta
            notificar_nova_leitura(sensor_id, sensor['nome'], sensor['tipo'], valor, unidade, 'UDP')
            if alerta:
                notificar_alerta(sensor_id, alerta, 'critical')
        except Exception:
            pass

        rtt_ms = 0
        ts_cliente = dados.get('timestamp_envio', 0)
        if ts_cliente > 0:
            rtt_ms = (t_recebido - ts_cliente) * 1000

        resposta = {
            'status': 'ok',
            'mensagem': f'Leitura UDP registrada: {valor}{unidade}',
            'protocolo': 'UDP',
            'seq_recebido': dados.get('seq', 0),
            'timestamp_servidor': t_recebido,
            'rtt_estimado_ms': round(rtt_ms, 2)
        }
        if alerta:
            resposta['alerta'] = alerta

        resp_bytes = json.dumps(resposta).encode()
        tamanho_req = len(dados_raw)
        registrar_log_rede(f"{endereco[0]}:{endereco[1]}", tamanho_req, len(resp_bytes), rtt_ms)

        logger.info(
            f"UDP LEITURA de {username}@{endereco[0]}: "
            f"sensor={sensor_id} valor={valor}{unidade} seq={dados.get('seq', '?')}"
        )
        return resp_bytes

    if tipo == 'batch':
        leituras = dados.get('leituras', [])
        salvos = 0
        erros = 0
        for l in leituras:
            sensor = verificar_sensor(l.get('sensor_id'), usuario_id)
            if sensor:
                salvar_leitura_udp(l['sensor_id'], l['valor'], l.get('unidade', ''))
                verificar_alerta(l['sensor_id'], sensor['tipo'], l['valor'])
                salvos += 1
            else:
                erros += 1

        resposta = {
            'status': 'ok',
            'mensagem': f'Batch UDP: {salvos} salvos, {erros} erros',
            'protocolo': 'UDP',
            'timestamp_servidor': t_recebido
        }
        resp_bytes = json.dumps(resposta).encode()
        logger.info(f"UDP BATCH de {username}@{endereco[0]}: {salvos} leituras")
        return resp_bytes

    return json.dumps({
        'status': 'erro',
        'mensagem': f'Tipo de mensagem desconhecido: {tipo}'
    }).encode()


def iniciar_servidor_udp():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((UDP_HOST, UDP_PORT))

    logger.info("=" * 60)
    logger.info(f"Servidor UDP rodando em {UDP_HOST}:{UDP_PORT}")
    logger.info("Aguardando datagramas de sensores...")
    logger.info("=" * 60)

    pacotes_recebidos = 0
    while True:
        try:
            dados, endereco = sock.recvfrom(BUFFER_SIZE)
            pacotes_recebidos += 1
            resposta = processar_mensagem(dados, endereco)
            sock.sendto(resposta, endereco)
        except KeyboardInterrupt:
            logger.info(f"\nServidor UDP encerrado. Total de pacotes: {pacotes_recebidos}")
            break
        except Exception as e:
            logger.error(f"Erro UDP: {e}")
            try:
                sock.sendto(json.dumps({
                    'status': 'erro',
                    'mensagem': str(e)
                }).encode(), endereco)
            except:
                pass

    sock.close()


def iniciar_udp_em_thread():
    thread = threading.Thread(target=iniciar_servidor_udp, daemon=True)
    thread.start()
    return thread


if __name__ == '__main__':
    iniciar_servidor_udp()
