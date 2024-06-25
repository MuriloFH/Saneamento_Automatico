from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def configuracaoRaisCampo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                          corrigirErros=False,
                          ordenacao_nulo=False
                          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_ordenacao_nulo(pre_validacao):
        nomeValidacao = "Ordenacao nulo"

        def analisa_ordenacao_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ordenacao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select ae.i_agrupadores 
                                            from bethadba.agrupadores_eventos ae 
                                            where ordenacao is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newOrdenacao = row['i_agrupadores']

                        dadoAlterado.append(f"Inserido a ordenação {newOrdenacao} para o agrupador {row['i_agrupadores']}")
                        comandoUpdate += f"UPDATE bethadba.agrupadores_eventos set ordenacao = {newOrdenacao} where i_agrupadores = {row['i_agrupadores']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ordenacao_nulo: {e}")
            return

        if ordenacao_nulo:
            dado = analisa_ordenacao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_ordenacao_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_ordenacao_nulo(pre_validacao="ordenacao_nulo")
