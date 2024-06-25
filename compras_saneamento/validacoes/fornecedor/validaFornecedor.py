import sys

from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, generateInscricaoMunicipal, geraInscricaoEstadual, find_emails, getICidade
import polars as pl


def fornecedores(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 fornecedor_endereco_incompleto=False,
                 fornecedor_inscricao_municipal_duplicada=False,
                 fornecedor_inscricao_estadual_duplicada=False,
                 fornecedor_cpf_invalido=False,
                 fornecedor_mais_de_um_email=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_fornecedor_endereco_incompleto(pre_validacao):
        nomeValidacao = "O endereço do fornecedor está incompleto"

        def analisa_fornecedor_endereco_incompleto():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_endereco_incompleto(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            listIcredores = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            try:
                listIcredores = ','.join([i_credor['i_chave_dsk1'] for i_credor in listDados])

                busca = banco.consultar(f"""select 
                                                i_credores,
                                                isnull(trim(endereco), '') as logradouro,
                                                c.i_cidades,
                                                upper(trim(cidade)) as cidade,
                                                i_entidades
                                            from compras.credores
                                            left join compras.compras.cidades c on (credores.cidade = c.nome)
                                            where i_credores in ({listIcredores})
                                            and credores.i_cidades is null
                                        """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        newCidade = row['i_cidades']

                        if row['i_cidades'] is None:
                            newCidade = getICidade(odbc=nomeOdbc, nomeCidade=row['cidade'], entidade=row['i_entidades'])

                        dadoAlterado.append(f"Alterado endereço do fornecedor {row['i_credores']} inserido a cidade {newCidade}")
                        comandoUpdate += f"""UPDATE compras.credores set i_cidades = {newCidade} where i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                        if row['logradouro'][0] == ',':
                            novo_endereco = row['logradouro'].replace(',', '')
                            dadoAlterado.append(f"Alterado endereço do fornecedor {row['i_credores']} para {novo_endereco}")
                            comandoUpdate += f"""UPDATE compras.credores set endereco = '{novo_endereco}' where i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_endereco_incompleto: {e}")
            return

        if fornecedor_endereco_incompleto:
            dado = analisa_fornecedor_endereco_incompleto()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_endereco_incompleto(listDados=dado)

    def analisa_corrige_fornecedor_inscricao_municipal_duplicada(pre_validacao):
        nomeValidacao = "Fornecedor com inscrição municipal duplicada"

        def analisa_fornecedor_inscricao_municipal_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_inscricao_municipal_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    for row in df.iter_rows(named=True):
                        new_inscricao_municipal = generateInscricaoMunicipal()
                        dadoAlterado.append(f"Alterada Inscrição Municipal do fornecedor {row['i_chave_dsk2']} para {new_inscricao_municipal}")
                        comandoUpdate += f"""UPDATE compras.credores set inscricao_municipal = '{new_inscricao_municipal}' where i_credores = {row['i_chave_dsk2']} and i_entidades = {row['i_chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_inscricao_municipal_duplicada: {e}")
            return

        if fornecedor_inscricao_municipal_duplicada:
            dado = analisa_fornecedor_inscricao_municipal_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_inscricao_municipal_duplicada(listDados=dado)

    def analisa_corrige_fornecedor_inscricao_estadual_duplicada(pre_validacao):
        nomeValidacao = "Fornecedor com inscrição estadual duplicada"

        def analisa_fornecedor_inscricao_estadual_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_inscricao_estadual_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:

                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    for row in df.iter_rows(named=True):
                        new_inscricao_estadual = geraInscricaoEstadual()
                        dadoAlterado.append(f"Alterada Inscrição Estadual do fornecedor {row['i_chave_dsk2']} para {new_inscricao_estadual}")
                        comandoUpdate += f"""UPDATE compras.credores set inscricao_estadual = '{new_inscricao_estadual}' where i_credores = {row['i_chave_dsk2']} and i_entidades = {row['i_chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_inscricao_estadual_duplicada: {e}")
            return

        if fornecedor_inscricao_estadual_duplicada:
            dado = analisa_fornecedor_inscricao_estadual_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_inscricao_estadual_duplicada(listDados=dado)

    def analisa_corrige_fornecedor_cpf_invalido(pre_validacao):
        nomeValidacao = "O CPF do fornecedor é inválido"

        def analisa_fornecedor_cpf_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    for row in df.iter_rows(named=True):
                        new_cpf = geraCfp()
                        dadoAlterado.append(f"Alterado CPF do fornecedor {row['i_chave_dsk2']} para {new_cpf}")
                        comandoUpdate += f"""UPDATE compras.credores set cpf = '{new_cpf}' where i_credores = {row['i_chave_dsk2']} and i_entidades = {row['i_chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_cpf_invalido: {e}")
            return

        if fornecedor_cpf_invalido:
            dado = analisa_fornecedor_cpf_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_cpf_invalido(listDados=dado)

    def analisa_corrige_fornecedor_mais_de_um_email(pre_validacao):
        nomeValidacao = "Fornecedor com mais de um e-mail"

        def analisa_fornecedor_mais_de_um_email():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_mais_de_um_email(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select 
                                                i_credores, 
                                                isnull(trim(compras.dbf_retira_acentos(email_fornecedor)),'') as email,
                                                charindex('@', email) AS primeiro,
                                                case 
                                                    when primeiro > 0
                                                    then charindex('@',substring(email, primeiro + 1))
                                                end as segundo,
                                                i_entidades
                                            from compras.credores
                                            where segundo > 0
                
                """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        novo_email = find_emails(row['email'])
                        dadoAlterado.append(f"Alterado email do fornecedor {row['i_credores']} de {row['email']} para {novo_email}")
                        comandoUpdate += f"""UPDATE compras.credores set email_fornecedor = '{novo_email}' where i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_mais_de_um_email: {e}")
            return

        if fornecedor_mais_de_um_email:
            dado = analisa_fornecedor_mais_de_um_email()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_mais_de_um_email(listDados=dado)

    if dadosList:
        analisa_corrige_fornecedor_endereco_incompleto(pre_validacao="fornecedor_endereco_incompleto")
        analisa_corrige_fornecedor_inscricao_municipal_duplicada(pre_validacao="fornecedor_inscricao_municipal_duplicada")
        analisa_corrige_fornecedor_inscricao_estadual_duplicada(pre_validacao="fornecedor_inscricao_estadual_duplicada")
        analisa_corrige_fornecedor_cpf_invalido(pre_validacao="fornecedor_cpf_invalido")
        analisa_corrige_fornecedor_mais_de_um_email(pre_validacao="fornecedor_mais_de_um_email")
