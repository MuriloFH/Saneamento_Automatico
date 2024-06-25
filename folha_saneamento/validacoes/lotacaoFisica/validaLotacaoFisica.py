from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import remove_caracteres_especiais


def lotacaoFisica(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  numero_telefone_maior_9_caracteres=False,
                  numero_telefone_maior_11_caracteres=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_numero_telefone_maior_9_caracteres(pre_validacao):
        nomeValidacao = "Lotação Física com número de telefone maior do que 9 caracteres."

        def analisa_numero_telefone_maior_9_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_numero_telefone_maior_9_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades,
                        i_locais_trab,
                        fone,
                        LENGTH(fone) AS quantidade
                    FROM 
                        bethadba.locais_trab
                    WHERE 
                        quantidade > 9   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        novo_fone = remove_caracteres_especiais(row['fone'])
                        if len(novo_fone) <= 9:
                            dadoAlterado.append(f"Alterado numero de telefone da lotação física {row['i_locais_trab']} entidade {row['i_entidades']} de {row['fone']} para {novo_fone}")
                            comandoUpdate += f"""UPDATE bethadba.locais_trab set fone = '{novo_fone}' where i_entidades = {row['i_entidades']} and i_locais_trab = {row['i_locais_trab']};\n"""
                        else:
                            novo_fone = novo_fone[-9:]
                            dadoAlterado.append(f"Alterado numero de telefone da lotação física {row['i_locais_trab']} entidade {row['i_entidades']} de {row['fone']} para {novo_fone}")
                            comandoUpdate += f"""UPDATE bethadba.locais_trab set fone = '{novo_fone}' where i_entidades = {row['i_entidades']} and i_locais_trab = {row['i_locais_trab']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_telefone_maior_9_caracteres: {e}")
            return

        if numero_telefone_maior_9_caracteres:
            dado = analisa_numero_telefone_maior_9_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_numero_telefone_maior_9_caracteres(listDados=dado)

    def analisa_corrige_numero_telefone_maior_11_caracteres(pre_validacao):
        nomeValidacao = "Lotação Física com número de telefone maior do que 11 caracteres."

        def analisa_numero_telefone_maior_11_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_numero_telefone_maior_11_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                            SELECT
                                                i_entidades,
                                                i_locais_trab,
                                                fone,
                                                LENGTH(fone) AS quantidade
                                            FROM
                                                bethadba.locais_trab
                                            WHERE quantidade > 11
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        novo_fone = remove_caracteres_especiais(row['fone'])
                        if len(novo_fone) <= 11:
                            dadoAlterado.append(f"Alterado numero de telefone da lotação física {row['i_locais_trab']} entidade {row['i_entidades']} de {row['fone']} para {novo_fone}")
                            comandoUpdate += f"""UPDATE bethadba.locais_trab set fone = '{novo_fone}' where i_entidades = {row['i_entidades']} and i_locais_trab = {row['i_locais_trab']};\n"""
                        else:
                            novo_fone = novo_fone[-11:]
                            dadoAlterado.append(f"Alterado numero de telefone da lotação física {row['i_locais_trab']} entidade {row['i_entidades']} de {row['fone']} para {novo_fone}")
                            comandoUpdate += f"""UPDATE bethadba.locais_trab set fone = '{novo_fone}' where i_entidades = {row['i_entidades']} and i_locais_trab = {row['i_locais_trab']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_telefone_maior_11_caracteres: {e}")
            return

        if numero_telefone_maior_11_caracteres:
            dado = analisa_numero_telefone_maior_11_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_numero_telefone_maior_11_caracteres(listDados=dado)

    if dadosList:
        analisa_corrige_numero_telefone_maior_9_caracteres(pre_validacao="numero_telefone_maior_9_caracteres")
        analisa_corrige_numero_telefone_maior_11_caracteres(pre_validacao='numero_telefone_maior_11_caracteres')
