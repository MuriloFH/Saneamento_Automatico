from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def rescisaoContratual(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       rescisao_contrato_vencido=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_rescisao_contrato_vencido(pre_validacao):
        nomeValidacao = "O contrato foi rescindido porem já havia vencido."

        def analisa_rescisao_contrato_vencido():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_rescisao_contrato_vencido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT 
                        c.i_entidades,
                        c.i_contratos,
                        (SELECT max(data_vcto) FROM compras.contratos WHERE i_contratos = c.i_contratos OR i_contratos_sup_compras = c.i_contratos) AS data_ultima_vig,
                        data_rescisao
                    FROM compras.contratos c
                    JOIN compras.rescisao_contratos r ON r.i_contratos = c.i_contratos AND r.i_entidades = c.i_entidades
                    WHERE data_rescisao > data_ultima_vig AND i_contratos_sup_compras IS NULL
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada da data de rescisao do contrato {row['i_contratos']} de {row['data_rescisao']} para {row['data_ultima_vig']}")
                        comandoUpdate += f"""UPDATE compras.rescisao_contratos set data_rescisao = '{row['data_ultima_vig']}' where i_contratos = {row['i_contratos']} and data_rescisao = '{row['data_rescisao']}' and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_rescisao_contrato_vencido: {e}")
            return

        if rescisao_contrato_vencido:
            dado = analisa_rescisao_contrato_vencido()

            if corrigirErros and len(dado) > 0:
                corrige_rescisao_contrato_vencido(listDados=dado)

    if dadosList:
        analisa_corrige_rescisao_contrato_vencido(pre_validacao="rescisao_contrato_vencido")
