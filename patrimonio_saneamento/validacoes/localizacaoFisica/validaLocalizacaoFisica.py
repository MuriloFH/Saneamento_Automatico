from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def localizacaoFisica(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                      corrigirErros=False,
                      descricao_duplicada=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_descricao_duplicada(pre_validacao):
        nomeValidacao = "Descrição duplicada"

        def analisa_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
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

        def corrige_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select lf.descricao , count(*) as quantidade, list(lf.i_localiz_fis) as id
                                                            from bethadba.localizacoes_fisicas lf
                                                            group by lf.descricao
                                                            HAVING count(*) > 1    
                                         """)

                if len(busca) > 0:
                    for i in busca:
                        a = i['id'].split(',')
                        for j in a:
                            dadoAlterado.append(f"Alterado a descrição da localização {j} para {j} | {i['descricao']}")
                            comandoUpdate += f"""update bethadba.localizacoes_fisicas set descricao = '{j} | {i['descricao']}' where i_localiz_fis in ({j});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_duplicada: {e}")
            return

        if descricao_duplicada:
            dado = analisa_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_duplicada(listDados=dado)

    if dadosList:
        analisa_corrige_descricao_duplicada(pre_validacao="descricao_duplicada")
    return
