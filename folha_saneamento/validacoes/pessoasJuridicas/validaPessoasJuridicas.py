from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCnpj, remove_caracteres_especiais
import polars as pl


def pessoasJuridicas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                     corrigirErros=False,
                     cnpj_nulo=False,
                     inscricao_municipal_superior_9_caracteres=False,
                     cnpj_invalido=False,
                     cnpj_duplicado=False
                     ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cnpj_nulo(pre_validacao):
        nomeValidacao = "Pessoa Jurídica com CNPJ nulo."

        def analisa_cnpj_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        pj.i_pessoas,
                        p.nome
                    FROM 
                        bethadba.pessoas_juridicas pj 
                    INNER JOIN 
                        bethadba.pessoas p ON (pj.i_pessoas = p.i_pessoas)
                    WHERE 
                        cnpj IS NULL    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        new_cnpj = geraCnpj()
                        dadoAlterado.append(f"Alterado CNPJ da pessoa jurídica {row['i_pessoas']} - {row['nome']} de nulo para {new_cnpj}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_juridicas set cnpj = '{new_cnpj}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_nulo: {e}")
            return

        if cnpj_nulo:
            dado = analisa_cnpj_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_nulo(listDados=dado)

    def analisa_corrige_inscricao_municipal_superior_9_caracteres(pre_validacao):
        nomeValidacao = "Inscrição municipal da pessoa jurídica possui mais de 9 digitos."

        def analisa_inscricao_municipal_superior_9_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_inscricao_municipal_superior_9_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_pessoas,
                        inscricao_municipal 
                    from bethadba.pessoas
                    where tipo_pessoa = 'J' 
                    and length(inscricao_municipal) > 9  
                 """)

                if len(busca) > 0:
                    for field in busca:
                        im = remove_caracteres_especiais(field['inscricao_municipal'])
                        dadoAlterado.append(f"Alterada Inscrição Municipal da pessoa jurídica {field['i_pessoas']} de {field['inscricao_municipal']} para {im if len(im) <= 9 else im[:9]}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas set inscricao_municipal = '{im if len(im) <= 9 else im[:9]}' where i_pessoas = {field['i_pessoas']} and tipo_pessoa = 'J';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_inscricao_municipal_superior_9_caracteres: {e}")
            return

        if inscricao_municipal_superior_9_caracteres:
            dado = analisa_inscricao_municipal_superior_9_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_inscricao_municipal_superior_9_caracteres(listDados=dado)

    def analisa_corrige_cnpj_invalido(pre_validacao):
        nomeValidacao = "Pessoa Jurídica com CNPJ nulo."

        def analisa_cnpj_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_pessoas,
                        cnpj
                    from bethadba.pessoas_juridicas
                    where cnpj is not null
                    and bethadba.dbf_valida_cgc_cpf(cnpj, null, 'J') = 0    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        new_cnpj = geraCnpj()
                        dadoAlterado.append(f"Alterado CNPJ da pessoa jurídica {row['i_pessoas']} de {row['cnpj']} para {new_cnpj}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_juridicas set cnpj = '{new_cnpj}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_invalido: {e}")
            return

        if cnpj_invalido:
            dado = analisa_cnpj_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_invalido(listDados=dado)

    def analisa_corrige_cnpj_duplicado(pre_validacao):
        nomeValidacao = "Pessoa Jurídica com CNPJ duplicado."

        def analisa_cnpj_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        list(p.i_pessoas) as ipessoas, 
                        pj.cnpj
                    from bethadba.pessoas p
                    join bethadba.pessoas_juridicas pj on p.i_pessoas = pj.i_pessoas
                    group by pj.cnpj
                    HAVING count(pj.cnpj) > 1    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_pj = row['ipessoas'].split(',')
                        for pessoa in list_pj[1:]:
                            new_cnpj = geraCnpj()
                            dadoAlterado.append(f"Alterado CNPJ da pessoa jurídica {pessoa} de {row['cnpj']} para {new_cnpj}")
                            comandoUpdate += f"""UPDATE bethadba.pessoas_juridicas set cnpj = '{new_cnpj}' where i_pessoas = {pessoa};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_duplicado: {e}")
            return

        if cnpj_duplicado:
            dado = analisa_cnpj_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_duplicado(listDados=dado)

    if dadosList:
        analisa_corrige_cnpj_nulo(pre_validacao="cnpj_nulo")
        analisa_corrige_inscricao_municipal_superior_9_caracteres(pre_validacao="inscricao_municipal_superior_9_caracteres")
        analisa_corrige_cnpj_invalido(pre_validacao="cnpj_invalido")
        analisa_corrige_cnpj_duplicado(pre_validacao="cnpj_duplicado")