from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def lancamentoOcorrencia(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                         corrigirErros=False,
                         ocorrencia_motorista_nulo=False
                         ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = "lancamentoOcorrencia"

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_ocorrencia_motorista_nulo(pre_validacao):
        nomeValidacao = "Ocorrencia sem motorista informado"
        preValidacaoBanco = pre_validacao

        def analisa_valida_corrige_ocorrencia_motorista_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            # print(dadosList)
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_valida_corrige_ocorrencia_motorista_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                newFuncionario = banco.consultar("""select first i_funcionarios as func from bethadba.funcionarios""")

                try:
                    for dados in listDados:

                        dadoAlterado.append(f"Adicionado o funcionarios {newFuncionario[0]['func']} no lançamento de ocorrencia {dados['i_chave_dsk1']}")

                    comandoUpdate += f"update bethadba.lanc_ocorr set i_funcionarios = {newFuncionario[0]['func']} where i_funcionarios is null;\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_ocorrencia_motorista_nulo {e}")

        if ocorrencia_motorista_nulo:
            dado = analisa_valida_corrige_ocorrencia_motorista_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_valida_corrige_ocorrencia_motorista_nulo(listDados=dado)

    if dadosList:
        valida_corrige_ocorrencia_motorista_nulo(pre_validacao='ocorrencia_motorista_nulo')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
