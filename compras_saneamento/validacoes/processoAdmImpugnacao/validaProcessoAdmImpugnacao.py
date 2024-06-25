from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def processoAdmImpugnacao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       data_julgamento_impugnacao_nula=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_julgamento_impugnacao_nula(pre_validacao):
        nomeValidacao = "A data de julgamento da impugnação não foi informada"

        def analisa_data_julgamento_impugnacao_nula():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_julgamento_impugnacao_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT DISTINCT
                       i_entidades,
                       i_impugnacoes_proc,
                       i_ano_proc,
                       i_processo,
                       i_credores,
                       data_impugnacao,
                       data_julgamento,
                       situacao
                    FROM  compras.impugnacoes_proc
                    WHERE  situacao <> 1
                    AND data_julgamento IS NULL
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de julgamento da impugnacao do processo {row['i_processo']}/{row['i_ano_proc']} Fornecedor {row['i_credores']}  para {row['data_impugnacao']}")
                        comandoUpdate += f"""UPDATE compras.impugnacoes_proc set data_julgamento = '{row['data_impugnacao']}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_impugnacoes_proc = {row['i_impugnacoes_proc']} and i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_julgamento_impugnacao_nula: {e}")
            return

        if data_julgamento_impugnacao_nula:
            dado = analisa_data_julgamento_impugnacao_nula()

            if corrigirErros and len(dado) > 0:
                corrige_data_julgamento_impugnacao_nula(listDados=dado)

    if dadosList:
        analisa_corrige_data_julgamento_impugnacao_nula(pre_validacao="data_julgamento_impugnacao_nula")
