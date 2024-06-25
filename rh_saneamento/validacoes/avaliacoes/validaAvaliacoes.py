from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def avaliacoes(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
               corrigirErros=False,
               local_avaliacao_bloco_nulo=False
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_local_avaliacao_bloco_nulo(pre_validacao):
        nomeValidacao = "Local de avaliação com bloco vazio"

        def analisa_local_avaliacao_bloco_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_local_avaliacao_bloco_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select * 
                                            from  bethadba.locais_aval
                                            where bloco is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o bloco 1 no local de avaliação {row['i_locais_aval']}-{row['descricao']}")
                        comandoUpdate += f"""UPDATE bethadba.locais_aval set bloco = 1 where i_pessoas = {row['i_pessoas']} and i_locais_aval = {row['i_locais_aval']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_local_avaliacao_bloco_nulo: {e}")
            return

        if local_avaliacao_bloco_nulo:
            dado = analisa_local_avaliacao_bloco_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_local_avaliacao_bloco_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_local_avaliacao_bloco_nulo(pre_validacao="local_avaliacao_bloco_nulo")
