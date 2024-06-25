import datetime
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def solicitacaoFornecimentoCompraDireta(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                                        corrigirErros=False,
                                        credor_af_diferente_da_compra_direta=False,
                                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_credor_af_diferente_da_compra_direta(pre_validacao):
        nomeValidacao = "Af possui credor diferente da compra direta"

        def analisa_credor_af_diferente_da_compra_direta():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_credor_af_diferente_da_compra_direta(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                resultado = banco.consultar(f"""SELECT sa.i_ano_sim,
                                                  sa.i_simples,
                                                  sa.i_ano_aut,
                                                  sa.i_sequ_aut,
                                                  sa.i_entidades,
                                                  sa.i_credores as credorAf,
                                                  af.i_credores as credorCompraDireta
                                               FROM compras.sequ_autor sa
                                               INNER JOIN compras.simples af
                                               ON (sa.i_ano_sim = af.i_ano_sim AND sa.i_simples = af.i_simples AND sa.i_entidades = af.i_entidades)
                                               WHERE
                                                  sa.i_credores <> af.i_credores
                                                  AND sa.i_ano_sim >= 1900
                                            """)
                for row in resultado:
                    # newCredor = row['credorAf']
                    # comandoUpdate += f"""UPDATE compras.compras.simples SET i_credores = {newCredor} WHERE i_ano_sim = {row['i_ano_sim']} and i_simples = {row['i_simples']} and i_entidades = {row['i_entidades']};\n"""
                    # dadoAlterado.append(f"Ajustado o credor da compra direta para {newCredor}")

                    newCredor = row['credorCompraDireta']
                    comandoUpdate += f"""UPDATE compras.sequ_autor set i_credores = {newCredor} where i_ano_aut = {row['i_ano_aut']} and i_sequ_aut = {row['i_sequ_aut']} and i_entidades = {row['i_entidades']};\n"""
                    dadoAlterado.append(f"Ajustado o credor da SF para {newCredor}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_credor_af_diferente_da_compra_direta: {e}")
            return

        if credor_af_diferente_da_compra_direta:
            dado = analisa_credor_af_diferente_da_compra_direta()

            if corrigirErros and len(dado) > 0:
                corrige_credor_af_diferente_da_compra_direta(listDados=dado)

    if dadosList:
        analisa_corrige_credor_af_diferente_da_compra_direta(pre_validacao='credor_af_diferente_da_compra_direta')
