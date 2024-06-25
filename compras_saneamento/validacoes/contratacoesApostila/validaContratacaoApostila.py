import datetime
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def contratacaoApostila(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        data_historico_fora_vigencia_contrato=False,
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_historico_fora_vigencia_contrato(pre_validacao):
        nomeValidacao = "Apostilamento com data de historico fora da vigência do contrato"

        def analisa_data_historico_fora_vigencia_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_historico_fora_vigencia_contrato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                resultado = banco.consultar(f"""SELECT c.i_contratos AS i_contratos,
                                                    h.i_sequ_hist AS i_sequ_hist,
                                                    c.i_entidades AS i_entidades,
                                                    c.data_ini_vig,
                                                    ISNULL(
                                                                (SELECT TOP 1 dateformat(ca.data_vcto, 'yyyy-mm-dd')
                                                                FROM compras.contratos ca
                                                                WHERE ca.i_contratos_sup_compras = c.i_contratos
                                                                AND ca.i_entidades = c.i_entidades
                                                                AND ca.data_vcto >= c.data_vcto
                                                                ORDER BY ca.data_vcto DESC), dateformat(c.data_vcto, 'yyyy-mm-dd')
                                                    ) AS ult_vcto, -- ultima data de vencimento
                                                    ISNULL(dateformat(h.data_historico, 'yyyy-mm-dd'), '') AS data_hist
                                                FROM compras.contratos c
                                                INNER JOIN compras.contratos_historico h ON h.i_entidades = c.i_entidades AND h.i_contratos = c.i_contratos
                                                WHERE h.tipo_historico in (6, 7, 10) -- apostilamentos
                                                AND data_hist NOT BETWEEN c.data_ini_vig AND ult_vcto
                                                AND natureza = 1
                                                AND i_ano_proc >= 1900
                                            """)
                for row in resultado:
                    newDataHistorico = row['data_ini_vig']
                    newDataHistorico += datetime.timedelta(days=1)
                    newDataHistorico = newDataHistorico.strftime('%Y-%m-%d')

                    dadoAlterado.append(f"Alterado a data do historico {row['i_sequ_hist']} para {newDataHistorico} do contrato {row['i_contratos']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE compras.contratos_historico set data_historico = '{newDataHistorico}' where i_contratos = {row['i_contratos']} and i_sequ_hist = {row['i_sequ_hist']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_historico_fora_vigencia_contrato: {e}")
            return

        if data_historico_fora_vigencia_contrato:
            dado = analisa_data_historico_fora_vigencia_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_data_historico_fora_vigencia_contrato(listDados=dado)

    if dadosList:
        analisa_corrige_data_historico_fora_vigencia_contrato(pre_validacao='data_historico_fora_vigencia_contrato')
