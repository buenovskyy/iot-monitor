"""
server iot monitoramento tempo real
funcionalidades:
api rest c http (Flask)
cadastro e autenticaçao de usuarios
dados sqlite
log
mediçao de rtt
tratamento de erros
comunicaçao em tempo real webwocket via socketIO
recepçao de dados via udp
"""

import os
import time
import json
import sqlite3
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, render_template, g

#config
app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)
DATABASE = 'iot_monitor.db'

#registro logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

#database
def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    conn = sqlite3.connect(DATABASE)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            token TEXT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sensores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,          -- temperatura, umidade, etc.
            localizacao TEXT,
            usuario_id INTEGER,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        );

        CREATE TABLE IF NOT EXISTS leituras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER NOT NULL,
            valor REAL NOT NULL,
            unidade TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            protocolo TEXT DEFAULT 'HTTP',   -- HTTP, UDP, WebSocket
            FOREIGN KEY (sensor_id) REFERENCES sensores(id)
        );

        CREATE TABLE IF NOT EXISTS alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER NOT NULL,
            mensagem TEXT NOT NULL,
            nivel TEXT DEFAULT 'warning',    -- info, warning, critical
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            lido INTEGER DEFAULT 0,
            FOREIGN KEY (sensor_id) REFERENCES sensores(id)
        );

        CREATE TABLE IF NOT EXISTS logs_rede (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origem TEXT,
            destino TEXT,
            protocolo TEXT,
            tamanho_bytes INTEGER,
            rtt_ms REAL,
            throughput_kbps REAL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    conn.close()
    logger.info("Banco de dados inicializado com sucesso.")

#auth
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def gerar_token():
    return secrets.token_hex(32)

def autenticacao_requerida(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            return jsonify({'erro': 'Token de autenticação não fornecido'}), 401
        db = get_db()
        usuario = db.execute(
            'SELECT * FROM usuarios WHERE token = ?', (token,)
        ).fetchone()
        if not usuario:
            return jsonify({'erro': 'Token inválido'}), 401
        g.usuario_id = usuario['id']
        g.username = usuario['username']
        return f(*args, **kwargs)
    return decorated

#medicao metricas
@app.before_request
def antes_request():
    g.inicio = time.time()

@app.after_request
def depois_request(response):
    if hasattr(g, 'inicio'):
        duracao_ms = (time.time() - g.inicio) * 1000
        tamanho = response.content_length or len(response.get_data())
        throughput = (tamanho * 8) / (duracao_ms / 1000) / 1000 if duracao_ms > 0 else 0

        #registra nobanco
        try:
            db = get_db()
            db.execute('''
                INSERT INTO logs_rede (origem, destino, protocolo, tamanho_bytes, rtt_ms, throughput_kbps)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                request.remote_addr,
                request.host,
                f'HTTP/{request.method}',
                tamanho,
                round(duracao_ms, 2),
                round(throughput, 2)
            ))
            db.commit()
        except Exception:
            pass

        #header
        response.headers['X-Response-Time-Ms'] = str(round(duracao_ms, 2))
        response.headers['X-Throughput-Kbps'] = str(round(throughput, 2))

        logger.info(
            f"{request.method} {request.path} -> {response.status_code} "
            f"| {duracao_ms:.1f}ms | {tamanho}B | {throughput:.1f}kbps"
        )
    return response

#rotas
@app.route('/')
def index():
    return render_template('dashboard.html')

#api rest
@app.route('/api/registrar', methods=['POST'])
def registrar():
    dados = request.get_json()
    if not dados or 'username' not in dados or 'password' not in dados:
        return jsonify({'erro': 'username e password são obrigatórios'}), 400

    db = get_db()
    try:
        token = gerar_token()
        db.execute(
            'INSERT INTO usuarios (username, password_hash, token) VALUES (?, ?, ?)',
            (dados['username'], hash_password(dados['password']), token)
        )
        db.commit()
        logger.info(f"Novo usuário registrado: {dados['username']}")
        return jsonify({
            'mensagem': 'Usuário registrado com sucesso',
            'token': token,
            'username': dados['username']
        }), 201
    except sqlite3.IntegrityError:
        return jsonify({'erro': 'Nome de usuário já existe'}), 409

@app.route('/api/login', methods=['POST'])
def login():
    dados = request.get_json()
    if not dados or 'username' not in dados or 'password' not in dados:
        return jsonify({'erro': 'username e password são obrigatórios'}), 400

    db = get_db()
    usuario = db.execute(
        'SELECT * FROM usuarios WHERE username = ? AND password_hash = ?',
        (dados['username'], hash_password(dados['password']))
    ).fetchone()

    if not usuario:
        return jsonify({'erro': 'Credenciais inválidas'}), 401

    #novo token por login
    novo_token = gerar_token()
    db.execute('UPDATE usuarios SET token = ? WHERE id = ?', (novo_token, usuario['id']))
    db.commit()
    logger.info(f"Login realizado: {dados['username']}")
    return jsonify({
        'mensagem': 'Login realizado com sucesso',
        'token': novo_token,
        'username': dados['username']
    })

#api rest sensores
@app.route('/api/sensores', methods=['POST'])
@autenticacao_requerida
def criar_sensor():
    dados = request.get_json()
    if not dados or 'nome' not in dados or 'tipo' not in dados:
        return jsonify({'erro': 'nome e tipo são obrigatórios'}), 400

    db = get_db()
    cursor = db.execute(
        'INSERT INTO sensores (nome, tipo, localizacao, usuario_id) VALUES (?, ?, ?, ?)',
        (dados['nome'], dados['tipo'], dados.get('localizacao', ''), g.usuario_id)
    )
    db.commit()
    logger.info(f"Sensor criado: {dados['nome']} (tipo: {dados['tipo']})")
    return jsonify({
        'mensagem': 'Sensor criado com sucesso',
        'sensor_id': cursor.lastrowid
    }), 201

@app.route('/api/sensores', methods=['GET'])
@autenticacao_requerida
def listar_sensores():
    db = get_db()
    sensores = db.execute(
        'SELECT * FROM sensores WHERE usuario_id = ?', (g.usuario_id,)
    ).fetchall()
    return jsonify([dict(s) for s in sensores])

#api rest dados
LIMITES_ALERTA = {
    'temperatura': {'min': 10, 'max': 40, 'unidade': '°C'},
    'umidade':     {'min': 20, 'max': 80, 'unidade': '%'},
    'pressao':     {'min': 950, 'max': 1050, 'unidade': 'hPa'},
}

def verificar_alerta(sensor_id, tipo, valor):
    limites = LIMITES_ALERTA.get(tipo)
    if not limites:
        return None

    db = get_db()
    if valor < limites['min']:
        msg = f"ALERTA: {tipo} muito baixo(a)! Valor: {valor}{limites['unidade']} (mín: {limites['min']})"
        nivel = 'critical'
    elif valor > limites['max']:
        msg = f"ALERTA: {tipo} muito alto(a)! Valor: {valor}{limites['unidade']} (máx: {limites['max']})"
        nivel = 'critical'
    elif valor > limites['max'] * 0.9 or valor < limites['min'] * 1.1:
        msg = f"AVISO: {tipo} próximo(a) do limite. Valor: {valor}{limites['unidade']}"
        nivel = 'warning'
    else:
        return None

    db.execute(
        'INSERT INTO alertas (sensor_id, mensagem, nivel) VALUES (?, ?, ?)',
        (sensor_id, msg, nivel)
    )
    db.commit()
    logger.warning(msg)
    return {'mensagem': msg, 'nivel': nivel}

@app.route('/api/leituras', methods=['POST'])
@autenticacao_requerida
def enviar_leitura():
    dados = request.get_json()
    campos = ['sensor_id', 'valor', 'unidade']
    if not dados or not all(c in dados for c in campos):
        return jsonify({'erro': f'Campos obrigatórios: {campos}'}), 400

    db = get_db()

    #verifica o usuario
    sensor = db.execute(
        'SELECT * FROM sensores WHERE id = ? AND usuario_id = ?',
        (dados['sensor_id'], g.usuario_id)
    ).fetchone()
    if not sensor:
        return jsonify({'erro': 'Sensor não encontrado'}), 404

    #salvar a leitura
    db.execute(
        'INSERT INTO leituras (sensor_id, valor, unidade, protocolo) VALUES (?, ?, ?, ?)',
        (dados['sensor_id'], dados['valor'], dados['unidade'], 'HTTP')
    )
    db.commit()

    #alertas automaticos
    alerta = verificar_alerta(dados['sensor_id'], sensor['tipo'], dados['valor'])

    #notificacao com websocket
    try:
        from ws_module import notificar_nova_leitura, notificar_alerta
        notificar_nova_leitura(
            dados['sensor_id'], sensor['nome'], sensor['tipo'],
            dados['valor'], dados['unidade'], 'HTTP'
        )
        if alerta:
            notificar_alerta(dados['sensor_id'], alerta['mensagem'], alerta['nivel'])
    except Exception:
        pass
    
    resposta = {'mensagem': 'Leitura registrada com sucesso', 'protocolo': 'HTTP'}
    if alerta:
        resposta['alerta'] = alerta

    return jsonify(resposta), 201

@app.route('/api/leituras/<int:sensor_id>', methods=['GET'])
@autenticacao_requerida
def consultar_leituras(sensor_id):
    limite = request.args.get('limite', 100, type=int)
    desde = request.args.get('desde', '')

    db = get_db()
    if desde:
        leituras = db.execute(
            'SELECT * FROM leituras WHERE sensor_id = ? AND timestamp >= ? ORDER BY timestamp DESC LIMIT ?',
            (sensor_id, desde, limite)
        ).fetchall()
    else:
        leituras = db.execute(
            'SELECT * FROM leituras WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT ?',
            (sensor_id, limite)
        ).fetchall()

    return jsonify([dict(l) for l in leituras])

#api alertas
@app.route('/api/alertas', methods=['GET'])
@autenticacao_requerida
def listar_alertas():
    db = get_db()
    alertas = db.execute('''
        SELECT a.* FROM alertas a
        JOIN sensores s ON a.sensor_id = s.id
        WHERE s.usuario_id = ?
        ORDER BY a.timestamp DESC LIMIT 50
    ''', (g.usuario_id,)).fetchall()
    return jsonify([dict(a) for a in alertas])

#api metricas da rede
@app.route('/api/metricas', methods=['GET'])
@autenticacao_requerida
def metricas_rede():
    limite = request.args.get('limite', 50, type=int)
    db = get_db()
    logs = db.execute(
        'SELECT * FROM logs_rede ORDER BY timestamp DESC LIMIT ?', (limite,)
    ).fetchall()

    #stats
    rtts = [l['rtt_ms'] for l in logs if l['rtt_ms']]
    throughputs = [l['throughput_kbps'] for l in logs if l['throughput_kbps']]

    stats = {}
    if rtts:
        stats['rtt'] = {
            'media_ms': round(sum(rtts) / len(rtts), 2),
            'min_ms': round(min(rtts), 2),
            'max_ms': round(max(rtts), 2),
            'amostras': len(rtts)
        }
    if throughputs:
        stats['throughput'] = {
            'media_kbps': round(sum(throughputs) / len(throughputs), 2),
            'min_kbps': round(min(throughputs), 2),
            'max_kbps': round(max(throughputs), 2),
            'amostras': len(throughputs)
        }

    return jsonify({
        'estatisticas': stats,
        'logs': [dict(l) for l in logs]
    })

@app.route('/api/ping', methods=['GET'])
def ping():
    return jsonify({
        'pong': True,
        'timestamp': datetime.now().isoformat(),
        'servidor': 'IoT Monitor v1.0'
    })

@app.route('/api/ws-stats', methods=['GET'])
def ws_statistics():
    try:
        from ws_module import get_ws_stats
        return jsonify(get_ws_stats())
    except Exception:
        return jsonify({'erro': 'WebSocket não inicializado'}), 503

#tratamento de erros
@app.errorhandler(404)
def nao_encontrado(e):
    return jsonify({'erro': 'Recurso não encontrado'}), 404

@app.errorhandler(500)
def erro_interno(e):
    logger.error(f"Erro interno: {e}")
    return jsonify({'erro': 'Erro interno do servidor'}), 500

@app.errorhandler(405)
def metodo_nao_permitido(e):
    return jsonify({'erro': 'Método HTTP não permitido'}), 405

#protcolo do app
@app.route('/api/protocolo', methods=['GET'])
def documentacao_protocolo():
    protocolo = {
        'nome': 'IoT Monitor Protocol (IMP/1.0)',
        'versao': '1.0',
        'descricao': 'Protocolo de aplicação para monitoramento IoT em tempo real',
        'transporte': ['TCP (HTTP/WebSocket)', 'UDP (dados de sensores)'],
        'formato_mensagens': 'JSON',
        'autenticacao': 'Bearer Token via header Authorization',
        'endpoints': {
            'POST /api/registrar': 'Cadastro de usuário',
            'POST /api/login': 'Autenticação (retorna token)',
            'POST /api/sensores': 'Cadastrar sensor (autenticado)',
            'GET  /api/sensores': 'Listar sensores (autenticado)',
            'POST /api/leituras': 'Enviar leitura HTTP (autenticado)',
            'GET  /api/leituras/<id>': 'Consultar leituras (autenticado)',
            'GET  /api/alertas': 'Listar alertas (autenticado)',
            'GET  /api/metricas': 'Métricas de rede (autenticado)',
            'GET  /api/ping': 'Medição de RTT (público)',
            'UDP :5006': 'Recepção de dados via UDP (Parte 2)',
            'WS  /socket.io': 'Comunicação tempo real (Parte 3)',
        },
        'formato_leitura': {
            'sensor_id': 'int (ID do sensor)',
            'valor': 'float (valor da medição)',
            'unidade': 'string (unidade de medida)',
        },
        'codigos_resposta': {
            200: 'OK - Requisição bem-sucedida',
            201: 'Created - Recurso criado',
            400: 'Bad Request - Dados inválidos',
            401: 'Unauthorized - Token inválido ou ausente',
            404: 'Not Found - Recurso não encontrado',
            409: 'Conflict - Recurso já existe',
            500: 'Internal Server Error',
        },
        'headers_customizados': {
            'X-Response-Time-Ms': 'Tempo de resposta em ms',
            'X-Throughput-Kbps': 'Throughput estimado em kbps',
        }
    }
    return jsonify(protocolo)

#init
if __name__ == '__main__':
    init_db()
    from ws_module import init_socketio
    socketio = init_socketio(app)

    from udp_server import iniciar_udp_em_thread
    iniciar_udp_em_thread()

    logger.info("=" * 60)
    logger.info("IoT Monitor Server v1.0")
    logger.info("Protocolo: IMP/1.0 (IoT Monitor Protocol)")
    logger.info("HTTP API rodando em http://0.0.0.0:5000")
    logger.info("WebSocket rodando em ws://0.0.0.0:5000/socket.io")
    logger.info("UDP receptor rodando na porta 5006")
    logger.info("=" * 60)
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)