from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def solicitacoesCompra(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       solicitacao_compra_possui_data_diferente_do_exercicio=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_solicitacao_compra_possui_data_diferente_do_exercicio(pre_validacao):
        nomeValidacao = "A solicitação de compra possui data diferente do exercício a qual se refere"

        def analisa_solicitacao_compra_possui_data_diferente_do_exercicio():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_solicitacao_compra_possui_data_diferente_do_exercicio(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select 
                                                i_requis, 
                                                i_ano, 
                                                i_entidades, 
                                                data_req, 
                                                cast(year(data_req) as int) as anoDataSolicitacao,
                                                cast(i_ano || RIGHT(data_req,6) as date) as nova_data
                                            from compras.requisicao 
                                            where year(data_req) <> i_ano   
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data da solicitação de compra {row['i_requis']}/{row['i_ano']} entidade {row['i_entidades']} de {row['data_req']} para {row['nova_data']}")
                        comandoUpdate += f"""update compras.requisicao set data_req = '{row['nova_data']}' where i_requis = {row['i_requis']} and i_ano = {row['i_ano']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_solicitacao_compra_possui_data_diferente_do_exercicio: {e}")
            return

        if solicitacao_compra_possui_data_diferente_do_exercicio:
            dado = analisa_solicitacao_compra_possui_data_diferente_do_exercicio()

            if corrigirErros and len(dado) > 0:
                corrige_solicitacao_compra_possui_data_diferente_do_exercicio(listDados=dado)

    if dadosList:
        analisa_corrige_solicitacao_compra_possui_data_diferente_do_exercicio(pre_validacao="solicitacao_compra_possui_data_diferente_do_exercicio")
