from utilitarios.conexao.conectaOdbc import Conecta
from compras_saneamento.populandoTabela.comandos import comandoComprasContratos
from rh_saneamento.populandoTabela.comandos import comandoRh
from ponto_saneamento.populandoTabela.comandos import comandoPonto
from folha_saneamento.populandoTabela.comandos import comandoFolha
from almoxarifado_saneamento.populandoTabela.comandos import comandoAlmoxarifado
from frotas_saneamento.populandoTabela.comandos import comandoFrotas
from patrimonio_saneamento.populandoTabela.comandos import comandoPatrimonio


def populaTabela(odbc, sistema=str):
    banco = Conecta(odbc=odbc)
    match sistema:
        case 'Compras':
            banco.executar(comandoComprasContratos())
        case 'Rh':
            banco.executar(comandoRh())
        case 'Ponto':
            banco.executar(comandoPonto())
        case 'Folha':
            banco.executar(comandoFolha())
        case 'Almoxarifado':
            banco.executar(comandoAlmoxarifado())
        case 'Frotas':
            banco.executar(comandoFrotas())
        case 'Patrimonio':
            banco.executar(comandoPatrimonio())
        case _: "Erro na função populaTabela. Insira um nome válido para o sistema"
