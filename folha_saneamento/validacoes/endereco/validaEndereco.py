from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def endereco(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             endereco_sem_numero=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_endereco_sem_numero(pre_validacao):
        nomeValidacao = "Endereço sem numero."

        def analisa_endereco_sem_numero():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_endereco_sem_numero(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_pessoas 
                    FROM 
                        bethadba.pessoas_enderecos 
                    WHERE
                        numero = ''  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado endereço da pessoa {row['i_pessoas']} de vazio para nulo")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_enderecos set numero = NULL where i_pessoas = {row['i_pessoas']} and numero = '';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_endereco_sem_numero: {e}")
            return

        if endereco_sem_numero:
            dado = analisa_endereco_sem_numero()

            if corrigirErros and len(dado) > 0:
                corrige_endereco_sem_numero(listDados=dado)

    if dadosList:
        analisa_corrige_endereco_sem_numero(pre_validacao="endereco_sem_numero")
