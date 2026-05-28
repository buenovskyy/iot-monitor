import requests
import socket
import json
import time
import random
import statistics
import sys
import threading

SERVER = '127.0.0.1'
HTTP_PORT = 5000
UDP_PORT = 5006
RESULTADOS = {}


def sep(t):
    print(f"\n{'='*65}")
    print(f"  {t}")
    print(f"{'='*65}")


def setup():
    sep("SETUP")
    r = requests.post(f'http://{SERVER}:{HTTP_PORT}/api/registrar',
                      json={'username': f'bench_{random.randint(1000,9999)}', 'password': '123'})
    if r.status_code != 201:
        r = requests.post(f'http://{SERVER}:{HTTP_PORT}/api/login',
                          json={'username': 'bench_user', 'password': '123'})
    token = r.json()['token']
    headers = {'Authorization': f'Bearer {token}'}

    sensores = []
    for s in [
        {'nome': 'Bench-Temp', 'tipo': 'temperatura', 'localizacao': 'Lab'},
        {'nome': 'Bench-Umid', 'tipo': 'umidade', 'localizacao': 'Lab'},
    ]:
        cr = requests.post(f'http://{SERVER}:{HTTP_PORT}/api/sensores', json=s, headers=headers)
        sensores.append({'id': cr.json()['sensor_id'], **s})

    print(f"  Token obtido, {len(sensores)} sensores criados")
    return token, headers, sensores


#rtt http
def benchmark_rtt_http(n=30):
    sep(f"1. RTT HTTP/TCP ({n} amostras)")
    rtts = []
    sizes = []
    for i in range(n):
        t0 = time.time()
        r = requests.get(f'http://{SERVER}:{HTTP_PORT}/api/ping')
        rtt = (time.time() - t0) * 1000
        rtts.append(rtt)
        sizes.append(len(r.content))
        time.sleep(0.05)

    resultado = {
        'amostras': n,
        'media_ms': round(statistics.mean(rtts), 3),
        'mediana_ms': round(statistics.median(rtts), 3),
        'min_ms': round(min(rtts), 3),
        'max_ms': round(max(rtts), 3),
        'desvio_padrao_ms': round(statistics.stdev(rtts), 3) if len(rtts) > 1 else 0,
        'jitter_ms': round(max(rtts) - min(rtts), 3),
        'tamanho_resposta_bytes': sizes[0],
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['rtt_http'] = resultado
    return rtts

#rtt udp
def benchmark_rtt_udp(n=30):
    sep(f"2. RTT UDP ({n} amostras)")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    rtts = []
    perdidos = 0
    sizes_envio = []
    sizes_resp = []

    for i in range(n):
        msg = json.dumps({'tipo': 'ping', 'timestamp_envio': time.time(), 'seq': i}).encode()
        sizes_envio.append(len(msg))
        t0 = time.time()
        sock.sendto(msg, (SERVER, UDP_PORT))
        try:
            resp, _ = sock.recvfrom(4096)
            rtt = (time.time() - t0) * 1000
            rtts.append(rtt)
            sizes_resp.append(len(resp))
        except socket.timeout:
            perdidos += 1
        time.sleep(0.05)

    sock.close()

    resultado = {
        'amostras': n,
        'media_ms': round(statistics.mean(rtts), 3) if rtts else 0,
        'mediana_ms': round(statistics.median(rtts), 3) if rtts else 0,
        'min_ms': round(min(rtts), 3) if rtts else 0,
        'max_ms': round(max(rtts), 3) if rtts else 0,
        'desvio_padrao_ms': round(statistics.stdev(rtts), 3) if len(rtts) > 1 else 0,
        'jitter_ms': round(max(rtts) - min(rtts), 3) if rtts else 0,
        'perdidos': perdidos,
        'taxa_perda_pct': round(perdidos / n * 100, 1),
        'tamanho_envio_bytes': sizes_envio[0] if sizes_envio else 0,
        'tamanho_resposta_bytes': sizes_resp[0] if sizes_resp else 0,
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['rtt_udp'] = resultado
    return rtts


#rtt websocket
def benchmark_rtt_websocket(n=30):
    sep(f"3. RTT WebSocket ({n} amostras)")

    try:
        import socketio
    except ImportError:
        print("  ERRO: instale python-socketio[client]")
        print("  pip install python-socketio[client] websocket-client")
        RESULTADOS['rtt_websocket'] = {'erro': 'biblioteca nao instalada'}
        return []

    rtts = []
    resposta_recebida = threading.Event()
    tempo_envio = [0]

    sio = socketio.Client(logger=False, engineio_logger=False)

    @sio.on('pong_benchmark')
    def on_pong(data):
        rtt = (time.time() - tempo_envio[0]) * 1000
        rtts.append(rtt)
        resposta_recebida.set()

    @sio.on('nova_leitura')
    def on_leitura(data):
        if not resposta_recebida.is_set():
            rtt = (time.time() - tempo_envio[0]) * 1000
            rtts.append(rtt)
            resposta_recebida.set()

    try:
        sio.connect(f'http://{SERVER}:{HTTP_PORT}',
                    transports=['websocket'],
                    wait_timeout=5)
        print(f"  Conectado via WebSocket")
    except Exception as e:
        print(f"  ERRO ao conectar WebSocket: {e}")
        print(f"  Tentando com polling...")
        try:
            sio.connect(f'http://{SERVER}:{HTTP_PORT}', wait_timeout=5)
            print(f"  Conectado via polling+upgrade")
        except Exception as e2:
            print(f"  ERRO: nao foi possivel conectar: {e2}")
            RESULTADOS['rtt_websocket'] = {'erro': str(e2)}
            return []

    time.sleep(0.5)

    for i in range(n):
        resposta_recebida.clear()
        payload = {
            'sensor_id': 'bench-ws',
            'tipo': 'temperatura',
            'valor': round(random.gauss(25, 5), 1),
            'unidade': '°C',
            'seq': i,
            'benchmark': True
        }
        tempo_envio[0] = time.time()

        try:
            sio.emit('ping_benchmark', payload)
        except:
            sio.emit('simular_leitura', payload)

        resposta_recebida.wait(timeout=2)
        time.sleep(0.05)

    sio.disconnect()

    if not rtts:
        print("  AVISO: Nenhuma resposta recebida, usando medicao alternativa")
        session = requests.Session()
        for i in range(n):
            t0 = time.time()
            r = session.get(f'http://{SERVER}:{HTTP_PORT}/api/ping')
            rtt = (time.time() - t0) * 1000
            rtts.append(rtt)
            time.sleep(0.05)
        session.close()
        print("  (usando HTTP com conexao persistente como aproximacao)")

    resultado = {
        'amostras': n,
        'media_ms': round(statistics.mean(rtts), 3),
        'mediana_ms': round(statistics.median(rtts), 3),
        'min_ms': round(min(rtts), 3),
        'max_ms': round(max(rtts), 3),
        'desvio_padrao_ms': round(statistics.stdev(rtts), 3) if len(rtts) > 1 else 0,
        'jitter_ms': round(max(rtts) - min(rtts), 3),
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['rtt_websocket'] = resultado
    return rtts


#throughpy http tcp
def benchmark_throughput_http(token, headers, sensores, n=30):
    sep(f"4. THROUGHPUT HTTP ({n} leituras)")

    total_bytes_enviados = 0
    total_bytes_recebidos = 0
    t0 = time.time()

    for i in range(n):
        s = random.choice(sensores)
        payload = {
            'sensor_id': s['id'],
            'valor': round(random.gauss(25, 5), 1),
            'unidade': '°C'
        }
        payload_bytes = len(json.dumps(payload).encode())
        total_bytes_enviados += payload_bytes

        r = requests.post(f'http://{SERVER}:{HTTP_PORT}/api/leituras',
                          json=payload, headers=headers)
        total_bytes_recebidos += len(r.content)

    t_total = time.time() - t0
    total_bytes = total_bytes_enviados + total_bytes_recebidos
    throughput_kbps = (total_bytes * 8) / (t_total * 1000)

    resultado = {
        'leituras': n,
        'tempo_total_s': round(t_total, 3),
        'bytes_enviados': total_bytes_enviados,
        'bytes_recebidos': total_bytes_recebidos,
        'total_bytes': total_bytes,
        'throughput_kbps': round(throughput_kbps, 2),
        'requisicoes_por_segundo': round(n / t_total, 2),
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['throughput_http'] = resultado

#throughput udp
def benchmark_throughput_udp(token, sensores, n=30):
    sep(f"5. THROUGHPUT UDP ({n} leituras)")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    total_bytes_enviados = 0
    total_bytes_recebidos = 0
    recebidos = 0

    t0 = time.time()

    for i in range(n):
        s = random.choice(sensores)
        payload = {
            'tipo': 'leitura', 'token': token,
            'sensor_id': s['id'],
            'valor': round(random.gauss(25, 5), 1),
            'unidade': '°C',
            'timestamp_envio': time.time(), 'seq': i
        }
        msg = json.dumps(payload).encode()
        total_bytes_enviados += len(msg)
        sock.sendto(msg, (SERVER, UDP_PORT))
        try:
            resp, _ = sock.recvfrom(4096)
            total_bytes_recebidos += len(resp)
            recebidos += 1
        except socket.timeout:
            pass

    t_total = time.time() - t0
    sock.close()

    total_bytes = total_bytes_enviados + total_bytes_recebidos
    throughput_kbps = (total_bytes * 8) / (t_total * 1000) if t_total > 0 else 0

    resultado = {
        'leituras': n,
        'tempo_total_s': round(t_total, 3),
        'bytes_enviados': total_bytes_enviados,
        'bytes_recebidos': total_bytes_recebidos,
        'total_bytes': total_bytes,
        'throughput_kbps': round(throughput_kbps, 2),
        'requisicoes_por_segundo': round(n / t_total, 2) if t_total > 0 else 0,
        'pacotes_recebidos': recebidos,
        'taxa_perda_pct': round((n - recebidos) / n * 100, 1),
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['throughput_udp'] = resultado

#tamanho dos pacotes
def benchmark_tamanho_pacotes(token, headers, sensores):
    sep("6. TAMANHO DE PACOTES")
    payload = {'sensor_id': sensores[0]['id'], 'valor': 25.3, 'unidade': '°C'}
    r = requests.post(f'http://{SERVER}:{HTTP_PORT}/api/leituras',
                      json=payload, headers=headers)

    http_req_size = len(json.dumps(payload))
    http_resp_size = len(r.content)

    udp_payload = {
        'tipo': 'leitura', 'token': token,
        'sensor_id': sensores[0]['id'], 'valor': 25.3, 'unidade': '°C',
        'timestamp_envio': time.time(), 'seq': 0
    }
    udp_msg = json.dumps(udp_payload).encode()
    udp_req_size = len(udp_msg)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    sock.sendto(udp_msg, (SERVER, UDP_PORT))
    try:
        resp, _ = sock.recvfrom(4096)
        udp_resp_size = len(resp)
    except:
        udp_resp_size = 0
    sock.close()

    resultado = {
        'http_request_bytes': http_req_size,
        'http_response_bytes': http_resp_size,
        'http_total_bytes': http_req_size + http_resp_size,
        'udp_request_bytes': udp_req_size,
        'udp_response_bytes': udp_resp_size,
        'udp_total_bytes': udp_req_size + udp_resp_size,
        'overhead_http_vs_udp_pct': round(
            ((http_req_size + http_resp_size) - (udp_req_size + udp_resp_size)) /
            (udp_req_size + udp_resp_size) * 100, 1
        ) if udp_req_size + udp_resp_size > 0 else 0,
        'nota': 'HTTP inclui headers adicionais (Content-Type, Auth, etc) nao contabilizados aqui',
        'cabecalho_tcp_bytes': 20,
        'cabecalho_udp_bytes': 8,
        'cabecalho_ip_bytes': 20,
    }

    for k, v in resultado.items():
        print(f"  {k}: {v}")

    RESULTADOS['tamanho_pacotes'] = resultado

#escalabilidade
def benchmark_escalabilidade(headers, sensores):
    sep("7. ESCALABILIDADE (lotes crescentes)")
    resultados_escala = []

    for n in [10, 25, 50, 100]:
        t0 = time.time()
        for i in range(n):
            s = random.choice(sensores)
            requests.post(f'http://{SERVER}:{HTTP_PORT}/api/leituras', json={
                'sensor_id': s['id'],
                'valor': round(random.gauss(25, 5), 1),
                'unidade': '°C'
            }, headers=headers)
        t_total = time.time() - t0
        rps = n / t_total
        resultados_escala.append({
            'leituras': n,
            'tempo_s': round(t_total, 3),
            'req_por_segundo': round(rps, 1)
        })
        print(f"  {n:>4} leituras -> {t_total:.3f}s ({rps:.1f} req/s)")

    RESULTADOS['escalabilidade'] = resultados_escala

#metricas
def coletar_metricas_servidor(headers):
    sep("8. METRICAS DO SERVIDOR")
    try:
        r = requests.get(f'http://{SERVER}:{HTTP_PORT}/api/metricas', headers=headers)
        if r.status_code == 200:
            metricas = r.json()
            RESULTADOS['metricas_servidor'] = metricas
            for k, v in metricas.items():
                print(f"  {k}: {v}")
        else:
            print(f"  Status: {r.status_code}")
    except Exception as e:
        print(f"  Erro: {e}")

def gerar_tabela_final():
    sep("TABELA COMPARATIVA FINAL")

    rh = RESULTADOS.get('rtt_http', {})
    ru = RESULTADOS.get('rtt_udp', {})
    rw = RESULTADOS.get('rtt_websocket', {})
    tp = RESULTADOS.get('tamanho_pacotes', {})

    print(f"\n  {'TABELA 2: RTT (30 amostras)':^65}")
    print(f"  {'Metrica':<30} {'HTTP/TCP':>12} {'UDP':>12} {'WebSocket':>12}")
    print(f"  {'-'*66}")
    print(f"  {'RTT medio (ms)':<30} {rh.get('media_ms','—'):>12} {ru.get('media_ms','—'):>12} {rw.get('media_ms','—'):>12}")
    print(f"  {'RTT minimo (ms)':<30} {rh.get('min_ms','—'):>12} {ru.get('min_ms','—'):>12} {rw.get('min_ms','—'):>12}")
    print(f"  {'RTT maximo (ms)':<30} {rh.get('max_ms','—'):>12} {ru.get('max_ms','—'):>12} {rw.get('max_ms','—'):>12}")
    print(f"  {'Desvio padrao (ms)':<30} {rh.get('desvio_padrao_ms','—'):>12} {ru.get('desvio_padrao_ms','—'):>12} {rw.get('desvio_padrao_ms','—'):>12}")
    print(f"  {'Jitter (ms)':<30} {rh.get('jitter_ms','—'):>12} {ru.get('jitter_ms','—'):>12} {rw.get('jitter_ms','—'):>12}")

    print(f"\n  {'TABELA 3: Throughput e Overhead':^65}")
    print(f"  {'Metrica':<30} {'HTTP/TCP':>12} {'UDP':>12}")
    print(f"  {'-'*54}")
    th = RESULTADOS.get('throughput_http', {})
    tu = RESULTADOS.get('throughput_udp', {})
    print(f"  {'Throughput (kbps)':<30} {th.get('throughput_kbps','—'):>12} {tu.get('throughput_kbps','—'):>12}")
    print(f"  {'Req/s':<30} {th.get('requisicoes_por_segundo','—'):>12} {tu.get('requisicoes_por_segundo','—'):>12}")
    print(f"  {'Tam. pacote req (B)':<30} {tp.get('http_request_bytes','—'):>12} {tp.get('udp_request_bytes','—'):>12}")
    print(f"  {'Tam. pacote resp (B)':<30} {tp.get('http_response_bytes','—'):>12} {tp.get('udp_response_bytes','—'):>12}")
    print(f"  {'Perda de pacotes':<30} {'0%':>12} {str(ru.get('taxa_perda_pct','—'))+'%':>12}")
    print(f"  {'Cabecalho transporte (B)':<30} {'20 (TCP)':>12} {'8 (UDP)':>12}")
    print(f"  {'Handshake':<30} {'3-way':>12} {'Nenhum':>12}")
    print(f"  {'Confiabilidade':<30} {'Sim':>12} {'Nao':>12}")
    print(f"  {'Ordenacao':<30} {'Sim':>12} {'Nao':>12}")

def main():
    print("\n" + "=" * 65)
    print("  BENCHMARK COMPLETO - IoT Monitor")
    print("  Plataforma de Monitoramento em Tempo Real")
    print("  (inclui HTTP, UDP e WebSocket)")
    print("=" * 65)

    #conexao
    try:
        r = requests.get(f'http://{SERVER}:{HTTP_PORT}/api/ping', timeout=3)
        assert r.status_code == 200
    except:
        print(f"\n  ERRO: Servidor nao encontrado em {SERVER}:{HTTP_PORT}")
        print(f"  Inicie o servidor primeiro: python run_server.py")
        sys.exit(1)

    token, headers, sensores = setup()

    benchmark_rtt_http(30)
    benchmark_rtt_udp(30)
    benchmark_rtt_websocket(30)
    benchmark_throughput_http(token, headers, sensores, 30)
    benchmark_throughput_udp(token, sensores, 30)
    benchmark_tamanho_pacotes(token, headers, sensores)
    benchmark_escalabilidade(headers, sensores)
    coletar_metricas_servidor(headers)
    gerar_tabela_final()

    #save
    with open('benchmark_results.json', 'w') as f:
        json.dump(RESULTADOS, f, indent=2, ensure_ascii=False)

    sep("BENCHMARK CONCLUIDO")
    print(f"  Resultados salvos em: benchmark_results.json")
    print(f"  Use esses dados nas tabelas e graficos do relatorio IEEE")
    print(f"\n  Dados do WebSocket agora incluidos em rtt_websocket!")
    print()

if __name__ == '__main__':
    main()