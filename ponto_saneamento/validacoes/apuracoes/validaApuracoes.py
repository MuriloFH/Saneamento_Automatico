from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def apuracoes(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
              corrigirErros=False,
              origem_marcacao_invalida=False
              ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_origem_marcacao_invalida(pre_validacao):
        nomeValidacao = "Origem marcação inválida"

        def analisa_origem_marcacao_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_origem_marcacao_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT i_entidades,
                                            i_sequencial,
                                            i_funcionarios,
                                            origem_marc
                                            FROM 
                                                bethadba.apuracoes_marc am
                                            WHERE origem_marc NOT IN ('O','I','A')
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado a origem_marc do funcionário {row['i_funcionarios']} para 'O - Original' da entidade {row['i_entidades']}")
                        comandoUpdate += f"UPDATE bethadba.apuracoes_marc set origem_marc = 'O' where i_funcionarios = {row['i_funcionarios']} and i_sequencial = {row['i_sequencial']} and i_entidades = {row['i_entidades']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_origem_marcacao_invalida: {e}")
            return

        if origem_marcacao_invalida:
            dado = analisa_origem_marcacao_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_origem_marcacao_invalida(listDados=dado)

    if dadosList:
        analisa_corrige_origem_marcacao_invalida(pre_validacao="origem_marcacao_invalida")
