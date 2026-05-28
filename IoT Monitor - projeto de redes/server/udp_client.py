"""
Cliente UDP - Simulador de Sensores IoT
CIC0124 - Redes de Computadores - UnB

Simula sensores enviando dados periodicamente via UDP.
Também realiza medições de RTT e detecta perda de pacotes.

Uso:
  python udp_client.py                    # modo padrão (20 leituras)
  python udp_client.py --leituras 50      # 50 leituras
  python udp_client.py --intervalo 0.5    # intervalo de 0.5s entre envios
  python udp_client.py --perda 10         # simula 10% de perda (não envia)
"""

import socket
import json
import time
import random
import argparse
import sys
import requests

SERVER_HOST = '127.0.0.1'
UDP_PORT = 5006
HTTP_PORT = 5000
TIMEOUT = 2  # segundos para esperar resposta UDP


def registrar_usuario_http():
    """Registra um usuário via HTTP e retorna o token."""
    username = f"sensor_udp_{random.randint(1000, 9999)}"
    try:
        r = requests.post(f'http://{SERVER_HOST}:{HTTP_PORT}/api/registrar', json={
            'username': username,
            'password': 'sensor123'
        })
        if r.status_code == 201:
            token = r.json()['token']
            print(f"  ✓ Usuário registrado: {username}")
            return token, username
        elif r.status_code == 409:
            # Já existe, faz login
            r = requests.post(f'http://{SERVER_HOST}:{HTTP_PORT}/api/login', json={
                'username': username,
                'password': 'sensor123'
            })
            return r.json()['token'], username
    except Exception as e:
        print(f"  ✗ Erro ao registrar via HTTP: {e}")
        sys.exit(1)


def criar_sensores_http(token):
    """Cria sensores via HTTP e retorna seus IDs."""
    headers = {'Authorization': f'Bearer {token}'}
    sensores = [
        {'nome': 'UDP-Temp-01', 'tipo': 'temperatura', 'localizacao': 'Sala Servidor'},
        {'nome': 'UDP-Umid-01', 'tipo': 'umidade', 'localizacao': 'Sala Servidor'},
        {'nome': 'UDP-Temp-02', 'tipo': 'temperatura', 'localizacao': 'Área Externa'},
    ]
    ids = []
    for s in sensores:
        r = requests.post(f'http://{SERVER_HOST}:{HTTP_PORT}/api/sensores',
                          json=s, headers=headers)
        if r.status_code == 201:
            sid = r.json()['sensor_id']
            ids.append({'id': sid, **s})
            print(f"  ✓ Sensor criado: {s['nome']} (ID: {sid})")
    return ids


def simular_valor(tipo):
    """Gera valor simulado baseado no tipo de sensor."""
    if tipo == 'temperatura':
        return round(random.gauss(25, 8), 1)  # média 25°C, desvio 8
    elif tipo == 'umidade':
        return round(random.gauss(55, 18), 1)  # média 55%, desvio 18
    elif tipo == 'pressao':
        return round(random.gauss(1013, 20), 1)
    return round(random.uniform(0, 100), 1)


def unidade_sensor(tipo):
    """Retorna a unidade baseado no tipo."""
    return {'temperatura': '°C', 'umidade': '%', 'pressao': 'hPa'}.get(tipo, '')


def medir_rtt_udp(sock, server_addr, n=10):
    """Mede RTT via UDP com múltiplas amostras de ping."""
    print(f"\n{'='*60}")
    print(f"  MEDIÇÃO DE RTT UDP ({n} amostras)")
    print(f"{'='*60}")

    rtts = []
    perdidos = 0

    for i in range(n):
        msg = json.dumps({
            'tipo': 'ping',
            'timestamp_envio': time.time(),
            'seq': i
        }).encode()

        t_inicio = time.time()
        sock.sendto(msg, server_addr)

        try:
            resp_raw, _ = sock.recvfrom(4096)
            t_fim = time.time()
            rtt = (t_fim - t_inicio) * 1000
            rtts.append(rtt)
            print(f"  Ping {i+1:3d}: RTT = {rtt:.2f} ms | {len(msg)}B enviado, {len(resp_raw)}B recebido")
        except socket.timeout:
            perdidos += 1
            print(f"  Ping {i+1:3d}: TIMEOUT (pacote perdido)")

        time.sleep(0.05)

    if rtts:
        print(f"\n  Resultados RTT UDP:")
        print(f"    Média:   {sum(rtts)/len(rtts):.2f} ms")
        print(f"    Mínimo:  {min(rtts):.2f} ms")
        print(f"    Máximo:  {max(rtts):.2f} ms")
        print(f"    Jitter:  {max(rtts) - min(rtts):.2f} ms")
        print(f"    Perdidos: {perdidos}/{n} ({perdidos/n*100:.1f}%)")

    return rtts, perdidos


def medir_rtt_http(n=10):
    """Mede RTT via HTTP/TCP para comparação."""
    print(f"\n{'='*60}")
    print(f"  MEDIÇÃO DE RTT HTTP/TCP ({n} amostras)")
    print(f"{'='*60}")

    rtts = []
    for i in range(n):
        t_inicio = time.time()
        try:
            r = requests.get(f'http://{SERVER_HOST}:{HTTP_PORT}/api/ping')
            t_fim = time.time()
            rtt = (t_fim - t_inicio) * 1000
            rtts.append(rtt)
            print(f"  Ping {i+1:3d}: RTT = {rtt:.2f} ms | Status: {r.status_code}")
        except Exception as e:
            print(f"  Ping {i+1:3d}: ERRO - {e}")
        time.sleep(0.05)

    if rtts:
        print(f"\n  Resultados RTT HTTP/TCP:")
        print(f"    Média:   {sum(rtts)/len(rtts):.2f} ms")
        print(f"    Mínimo:  {min(rtts):.2f} ms")
        print(f"    Máximo:  {max(rtts):.2f} ms")
        print(f"    Jitter:  {max(rtts) - min(rtts):.2f} ms")

    return rtts


def comparar_tcp_udp(rtts_udp, rtts_http, perdidos_udp, n_udp):
    """Gera relatório comparativo TCP vs UDP."""
    print(f"\n{'='*60}")
    print(f"  COMPARAÇÃO TCP vs UDP")
    print(f"{'='*60}")

    if rtts_udp and rtts_http:
        media_udp = sum(rtts_udp) / len(rtts_udp)
        media_http = sum(rtts_http) / len(rtts_http)

        print(f"\n  {'Métrica':<25} {'UDP':>12} {'HTTP/TCP':>12}")
        print(f"  {'-'*25} {'-'*12} {'-'*12}")
        print(f"  {'RTT médio (ms)':<25} {media_udp:>12.2f} {media_http:>12.2f}")
        print(f"  {'RTT mínimo (ms)':<25} {min(rtts_udp):>12.2f} {min(rtts_http):>12.2f}")
        print(f"  {'RTT máximo (ms)':<25} {max(rtts_udp):>12.2f} {max(rtts_http):>12.2f}")
        print(f"  {'Jitter (ms)':<25} {max(rtts_udp)-min(rtts_udp):>12.2f} {max(rtts_http)-min(rtts_http):>12.2f}")
        print(f"  {'Perda de pacotes':<25} {perdidos_udp/n_udp*100:>11.1f}% {'0.0':>11}%")
        print(f"  {'Confiabilidade':<25} {'Não':>12} {'Sim':>12}")
        print(f"  {'Handshake':<25} {'Não':>12} {'3-way':>12}")
        print(f"  {'Overhead':<25} {'Baixo':>12} {'Alto':>12}")

        if media_udp < media_http:
            diff = ((media_http - media_udp) / media_http) * 100
            print(f"\n  → UDP foi {diff:.1f}% mais rápido que HTTP/TCP")
        else:
            diff = ((media_udp - media_http) / media_udp) * 100
            print(f"\n  → HTTP/TCP foi {diff:.1f}% mais rápido (possível em localhost)")

        print(f"\n  Análise:")
        print(f"    - UDP não realiza handshake (3-way TCP), reduzindo latência")
        print(f"    - TCP garante entrega e ordenação, UDP não")
        print(f"    - UDP tem menor overhead de cabeçalho (8B vs 20B+ TCP)")
        print(f"    - Para IoT com alta frequência, UDP é mais eficiente")
        print(f"    - Para dados críticos, TCP é mais confiável")


def enviar_leituras_udp(sock, server_addr, token, sensores, n_leituras, intervalo, simular_perda):
    """Envia leituras simuladas via UDP."""
    print(f"\n{'='*60}")
    print(f"  ENVIO DE LEITURAS VIA UDP ({n_leituras} leituras)")
    print(f"{'='*60}")

    enviados = 0
    recebidos = 0
    perdidos_sim = 0
    perdidos_real = 0
    rtts = []
    bytes_enviados = 0
    bytes_recebidos = 0
    t_total_inicio = time.time()

    for i in range(n_leituras):
        sensor = random.choice(sensores)
        valor = simular_valor(sensor['tipo'])
        unidade = unidade_sensor(sensor['tipo'])

        msg = json.dumps({
            'tipo': 'leitura',
            'token': token,
            'sensor_id': sensor['id'],
            'valor': valor,
            'unidade': unidade,
            'timestamp_envio': time.time(),
            'seq': i
        }).encode()

        # Simula perda de pacotes (não envia)
        if simular_perda > 0 and random.randint(1, 100) <= simular_perda:
            perdidos_sim += 1
            print(f"  [{i+1:3d}] ✗ SIMULADO: pacote não enviado (perda simulada)")
            continue

        t_inicio = time.time()
        sock.sendto(msg, server_addr)
        enviados += 1
        bytes_enviados += len(msg)

        try:
            resp_raw, _ = sock.recvfrom(4096)
            t_fim = time.time()
            rtt = (t_fim - t_inicio) * 1000
            rtts.append(rtt)
            recebidos += 1
            bytes_recebidos += len(resp_raw)

            resp = json.loads(resp_raw.decode())
            alerta_str = " ⚠️" if 'alerta' in resp else ""
            print(
                f"  [{i+1:3d}] ✓ {sensor['nome']}: {valor}{unidade} "
                f"| RTT: {rtt:.1f}ms | seq:{i}{alerta_str}"
            )
        except socket.timeout:
            perdidos_real += 1
            print(f"  [{i+1:3d}] ✗ TIMEOUT: sem resposta do servidor")

        time.sleep(intervalo)

    t_total = time.time() - t_total_inicio

    print(f"\n  Resumo do envio UDP:")
    print(f"    Pacotes enviados:   {enviados}")
    print(f"    Respostas recebidas: {recebidos}")
    print(f"    Perda real:         {perdidos_real}")
    print(f"    Perda simulada:     {perdidos_sim}")
    print(f"    Bytes enviados:     {bytes_enviados}")
    print(f"    Bytes recebidos:    {bytes_recebidos}")
    print(f"    Tempo total:        {t_total:.2f}s")
    if t_total > 0:
        throughput = (bytes_enviados + bytes_recebidos) * 8 / t_total / 1000
        print(f"    Throughput:         {throughput:.2f} kbps")
    if rtts:
        print(f"    RTT médio:          {sum(rtts)/len(rtts):.2f} ms")

    return rtts


def enviar_batch_udp(sock, server_addr, token, sensores):
    """Envia múltiplas leituras em um único datagrama (batch)."""
    print(f"\n{'='*60}")
    print(f"  ENVIO BATCH UDP (múltiplas leituras em 1 datagrama)")
    print(f"{'='*60}")

    leituras = []
    for _ in range(10):
        sensor = random.choice(sensores)
        leituras.append({
            'sensor_id': sensor['id'],
            'valor': simular_valor(sensor['tipo']),
            'unidade': unidade_sensor(sensor['tipo'])
        })

    msg = json.dumps({
        'tipo': 'batch',
        'token': token,
        'leituras': leituras,
        'timestamp_envio': time.time()
    }).encode()

    print(f"  Enviando batch com {len(leituras)} leituras ({len(msg)} bytes)...")

    t_inicio = time.time()
    sock.sendto(msg, server_addr)

    try:
        resp_raw, _ = sock.recvfrom(4096)
        rtt = (time.time() - t_inicio) * 1000
        resp = json.loads(resp_raw.decode())
        print(f"  ✓ Resposta: {resp['mensagem']}")
        print(f"  RTT: {rtt:.2f} ms | {len(msg)}B enviado, {len(resp_raw)}B recebido")
    except socket.timeout:
        print(f"  ✗ TIMEOUT: sem resposta")


def main():
    parser = argparse.ArgumentParser(description='Cliente UDP - Simulador de Sensores IoT')
    parser.add_argument('--leituras', type=int, default=20, help='Número de leituras (padrão: 20)')
    parser.add_argument('--intervalo', type=float, default=0.3, help='Intervalo entre envios em segundos (padrão: 0.3)')
    parser.add_argument('--perda', type=int, default=0, help='Percentual de perda simulada (padrão: 0)')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='IP do servidor')
    args = parser.parse_args()

    global SERVER_HOST
    SERVER_HOST = args.host
    server_addr = (SERVER_HOST, UDP_PORT)

    print(f"\n{'='*60}")
    print(f"  CLIENTE UDP - Simulador de Sensores IoT")
    print(f"  Servidor: {SERVER_HOST}:{UDP_PORT} (UDP) / {HTTP_PORT} (HTTP)")
    print(f"{'='*60}")

    # 1. Registrar usuário e criar sensores (via HTTP)
    print(f"\n--- Setup via HTTP ---")
    token, username = registrar_usuario_http()
    sensores = criar_sensores_http(token)

    if not sensores:
        print("  ✗ Nenhum sensor criado. Abortando.")
        sys.exit(1)

    # 2. Criar socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)

    # 3. Medir RTT (UDP)
    rtts_udp, perdidos_udp = medir_rtt_udp(sock, server_addr, n=10)

    # 4. Medir RTT (HTTP/TCP) para comparação
    rtts_http = medir_rtt_http(n=10)

    # 5. Comparação TCP vs UDP
    comparar_tcp_udp(rtts_udp, rtts_http, perdidos_udp, 10)

    # 6. Enviar leituras
    enviar_leituras_udp(sock, server_addr, token, sensores,
                        args.leituras, args.intervalo, args.perda)

    # 7. Enviar batch
    enviar_batch_udp(sock, server_addr, token, sensores)

    sock.close()
    print(f"\n{'='*60}")
    print(f"  ✅ CLIENTE UDP FINALIZADO")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
