from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def rua(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             rua_sem_descricao=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_rua_sem_descricao(pre_validacao):
        nomeValidacao = "Rua sem nome."

        def analisa_rua_sem_descricao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_rua_sem_descricao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_ruas, nome
                    FROM 
                        bethadba.ruas 
                    WHERE
                        nome = '' OR
                        nome IS NULL  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado nome da rua {row['i_ruas']} para Rua sem nome {row['i_ruas']}")
                        comandoUpdate += f"""UPDATE bethadba.ruas set nome = 'Rua sem nome {row['i_ruas']}' where i_ruas = {row['i_ruas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_rua_sem_descricao: {e}")
            return

        if rua_sem_descricao:
            dado = analisa_rua_sem_descricao()

            if corrigirErros and len(dado) > 0:
                corrige_rua_sem_descricao(listDados=dado)

    if dadosList:
        analisa_corrige_rua_sem_descricao(pre_validacao="rua_sem_descricao")
