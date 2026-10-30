IP = "192.168.5.91"
PORTA = 4842
VALOR_PROCURADO = 79

# Configurações opcionais
USUARIO = None          # ex: "admin"
SENHA = None            # ex: "1234"
TOLERANCIA_FLOAT = 1e-6 # tolerância para comparar números decimais
PROCURAR_EM_ARRAYS = True
NO_INICIAL = "ns=4;s=APPL"  # variáveis da aplicação; use "i=85" (Objects) para o servidor inteiro
TIMEOUT = 30            # segundos por requisição
TAMANHO_LOTE = 100      # quantas variáveis ler por requisição
MAX_TENTATIVAS = 5      # tentativas de reconexão por operação

"""
Conecta a um servidor OPC UA, percorre todos os nós a partir de NO_INICIAL
e lista as variáveis cujo valor é igual a VALOR_PROCURADO.

Para não sobrecarregar o servidor (ex: CLPs), o nome e a classe de cada nó
vêm na própria resposta do browse e os valores são lidos em lote. Se a
conexão cair, o script reconecta e continua de onde parou.

Dependência:  pip install asyncua
"""

import asyncio
import logging
from collections import deque

from asyncua import Client, ua

logging.getLogger("asyncua").setLevel(logging.CRITICAL)


def valores_iguais(valor, procurado):
    if isinstance(valor, (list, tuple)):
        return PROCURAR_EM_ARRAYS and any(valores_iguais(v, procurado) for v in valor)

    # bool antes de número, pois bool é subclasse de int
    if isinstance(procurado, bool) or isinstance(valor, bool):
        return isinstance(valor, bool) and isinstance(procurado, bool) and valor == procurado

    if isinstance(procurado, (int, float)) and isinstance(valor, (int, float)):
        return abs(float(valor) - float(procurado)) <= TOLERANCIA_FLOAT

    if isinstance(valor, ua.LocalizedText):
        valor = valor.Text

    return str(valor) == str(procurado)


class Conexao:
    """Mantém o cliente conectado e repete a operação se a conexão cair."""

    def __init__(self, url):
        self.url = url
        self.client = None

    async def conectar(self):
        """Conecta, tentando de novo com espera crescente (o servidor pode
        demorar a liberar a sessão anterior)."""
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            await self.desconectar()
            # watchdog_intervall padrão (1s) é curto demais para CLPs ocupados e
            # derruba a conexão quando o servidor demora a responder
            client = Client(url=self.url, timeout=TIMEOUT, watchdog_intervall=TIMEOUT)
            if USUARIO:
                client.set_user(USUARIO)
                client.set_password(SENHA or "")
            try:
                await client.connect()
                self.client = client
                return
            except Exception as e:
                espera = 5 * tentativa
                print(f"  ! falha ao conectar ({type(e).__name__}: {e}) "
                      f"[{tentativa}/{MAX_TENTATIVAS}], nova tentativa em {espera}s ...")
                await asyncio.sleep(espera)
        raise ConnectionError(f"não foi possível conectar a {self.url}")

    async def desconectar(self):
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

    async def executar(self, operacao):
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                if self.client is None:
                    await self.conectar()
                return await operacao(self.client)
            except (ConnectionError, asyncio.TimeoutError, OSError) as e:
                print(f"  ! conexão perdida ({type(e).__name__}), reconectando "
                      f"[{tentativa}/{MAX_TENTATIVAS}] ...")
                await self.desconectar()
                await asyncio.sleep(2)
        raise ConnectionError("não foi possível restabelecer a conexão")


async def procurar(conexao, encontrados):
    """Percorre a árvore e compara os valores das variáveis em lotes, durante a busca."""
    visitados = {NO_INICIAL}
    fila = deque([(ua.NodeId.from_string(NO_INICIAL), "")])
    pendentes = []
    total_variaveis = 0

    async def verificar(lote):
        for (nodeid, caminho), dv in zip(lote, await ler_valores(conexao, lote)):
            if dv is None or not dv.StatusCode.is_good() or dv.Value is None:
                continue
            valor = dv.Value.Value
            if valores_iguais(valor, VALOR_PROCURADO):
                encontrados.append((nodeid.to_string(), caminho, valor))
                print(f"[ENCONTRADO] {caminho}  ({nodeid.to_string()}) = {valor!r}")

    while fila:
        nodeid, caminho = fila.popleft()

        async def browse(client):
            return await client.get_node(nodeid).get_children_descriptions()

        try:
            filhos = await conexao.executar(browse)
        except ua.UaError:
            filhos = []  # nó com erro no servidor: ignora só ele

        for ref in filhos:
            filho_id = ref.NodeId
            chave = filho_id.to_string()
            if chave in visitados:
                continue
            visitados.add(chave)

            caminho_filho = f"{caminho}/{ref.BrowseName.Name}"
            if ref.NodeClass == ua.NodeClass.Variable:
                pendentes.append((filho_id, caminho_filho))
                total_variaveis += 1
            fila.append((filho_id, caminho_filho))

        while len(pendentes) >= TAMANHO_LOTE or (pendentes and not fila):
            lote, pendentes = pendentes[:TAMANHO_LOTE], pendentes[TAMANHO_LOTE:]
            await verificar(lote)

        if len(visitados) % 2000 < len(filhos):
            print(f"  ... {len(visitados)} nós percorridos, {total_variaveis} variáveis")


async def ler_valores(conexao, lote):
    async def ler(client):
        nos = [client.get_node(nodeid) for nodeid, _ in lote]
        return await client.read_attributes(nos, ua.AttributeIds.Value)

    try:
        return await conexao.executar(ler)
    except ua.UaError:
        if len(lote) == 1:
            return [None]
        # lote recusado pelo servidor: lê um por um
        resultados = []
        for item in lote:
            resultados.extend(await ler_valores(conexao, [item]))
        return resultados


async def main():
    url = f"opc.tcp://{IP}:{PORTA}"
    print(f"Conectando a {url} ...")
    conexao = Conexao(url)
    await conexao.conectar()

    encontrados = []
    try:
        print(f"Conectado. Procurando {VALOR_PROCURADO!r} a partir de {NO_INICIAL} ...\n")
        await procurar(conexao, encontrados)
    finally:
        await conexao.desconectar()

    print(f"\nTotal encontrado: {len(encontrados)}")
    for nodeid, caminho, valor in encontrados:
        print(f"  {nodeid:40s} {caminho}  = {valor!r}")


if __name__ == "__main__":
    asyncio.run(main())
