from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def vinculoEmpregaticio(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        categoria_esocial_nulo=False,
                        duplicidade_vinculo_empregaticio=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_categoria_esocial_nulo(pre_validacao):
        nomeValidacao = "Categoria eSocial nulo no vínculo empregatício"

        def analisa_categoria_esocial_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_categoria_esocial_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_vinculos,
                        descricao,
                        categoria_esocial,
                        tipo_func,
                        case tipo_func 
                            WHEN 'A' then 701
                            WHEN 'F' then 101
                            ELSE null
                        end as nova_categ
                    FROM 
                        bethadba.vinculos
                    WHERE 
                        categoria_esocial IS NULL
                        AND tipo_func <> 'B'   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada Categoria eSocial do vínculo {row['i_vinculos']} tipo de funcionário {row['tipo_func']} para {row['nova_categ']}")
                        comandoUpdate += f"""UPDATE bethadba.vinculos set categoria_esocial = '{row['nova_categ']}' where i_vinculos = {row['i_vinculos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_categoria_esocial_nulo: {e}")
            return

        if categoria_esocial_nulo:
            dado = analisa_categoria_esocial_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_categoria_esocial_nulo(listDados=dado)

    def analisa_corrige_duplicidade_vinculo_empregaticio(pre_validacao):
        nomeValidacao = "Duplicidade de Vinculo empregatício."

        def analisa_duplicidade_vinculo_empregaticio():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_duplicidade_vinculo_empregaticio(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_vinculos) as vinculos, 
                        trim(descricao) as descricao,
                        count(descricao) AS quantidade 
                    FROM 
                        bethadba.vinculos 
                    GROUP BY 
                        descricao 
                    HAVING
                        quantidade > 1   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_vinc = row['vinculos'].split(',')
                        for vinc in list_vinc[1:]:
                            novo_nome = row['descricao'] + ' ' + vinc
                            dadoAlterado.append(f"Alterada descrição do vinculo empregatício ({vinc} - {row['descricao']}) para {novo_nome}")
                            comandoUpdate += f"""UPDATE bethadba.vinculos set descricao = '{novo_nome}' where i_vinculos = {vinc};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_duplicidade_vinculo_empregaticio: {e}")
            return

        if duplicidade_vinculo_empregaticio:
            dado = analisa_duplicidade_vinculo_empregaticio()

            if corrigirErros and len(dado) > 0:
                corrige_duplicidade_vinculo_empregaticio(listDados=dado)

    if dadosList:
        analisa_corrige_categoria_esocial_nulo(pre_validacao="categoria_esocial_nulo")
        analisa_corrige_duplicidade_vinculo_empregaticio(pre_validacao="duplicidade_vinculo_empregaticio")
