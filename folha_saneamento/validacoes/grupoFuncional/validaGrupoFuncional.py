from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def grupoFuncional(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   grupos_funcionais_duplicados=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_grupos_funcionais_duplicados(pre_validacao):
        nomeValidacao = "Grupo Funcional duplicado."

        def analisa_grupos_funcionais_duplicados():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_grupos_funcionais_duplicados(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        list(i_entidades) as entidades,
                        list(i_grupos) as grupos,
                        nome,
                        count(nome) AS quantidade
                    FROM 
                        bethadba.grupos
                    GROUP BY 
                        nome 
                    HAVING 
                        quantidade > 1   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_grupo = row['grupos'].split(',')
                        list_entidade = row['entidades'].split(',')
                        for i, grupo in enumerate(list_grupo):
                            novo_nome = row['nome'] + ' ' + grupo + list_entidade[i]
                            if len(novo_nome) <= 50:
                                dadoAlterado.append(f"Alterada descrição do grupo funcional {grupo}-{row['nome']} entidade {list_entidade[i]} para {novo_nome}")
                                comandoUpdate += f"""UPDATE bethadba.grupos set nome = '{novo_nome}' where i_entidades = {list_entidade[i]} and i_grupos = {grupo};\n"""
                            else:
                                novo = row['nome'][:50 - len(grupo) - len(list_entidade[i])] + grupo + list_entidade[i]
                                dadoAlterado.append(f"Alterada descrição do grupo funcional {grupo}-{row['nome']} entidade {list_entidade[i]} para {novo}")
                                comandoUpdate += f"""UPDATE bethadba.grupos set nome = '{novo}' where i_entidades = {list_entidade[i]} and i_grupos = {grupo};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_grupos_funcionais_duplicados: {e}")
            return

        if grupos_funcionais_duplicados:
            dado = analisa_grupos_funcionais_duplicados()

            if corrigirErros and len(dado) > 0:
                corrige_grupos_funcionais_duplicados(listDados=dado)

    if dadosList:
        analisa_corrige_grupos_funcionais_duplicados(pre_validacao="grupos_funcionais_duplicados")
