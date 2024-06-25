import polars as pl
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, geraPis, generateRG


def pessoasFisicas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   cpf_duplicado=False,
                   pis_duplicado=False,
                   pis_invalido=False,
                   rg_duplicado=False,
                   data_nascimento_nulo=False,
                   filiacao_duplicada=False,
                   email_invalido=False,
                   dependente_mesma_pessoa_responsavel=False,
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cpf_duplicado(pre_validacao):
        nomeValidacao = "Pessoa Física com CPF duplicado"

        def analisa_cpf_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        list(pf.i_pessoas) as ipessoa,
                        cpf,
                        count(cpf) AS quantidade
                    FROM 
                        bethadba.pessoas_fisicas pf 
                    GROUP BY 
                        cpf 
                    HAVING 
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        list_pessoas = row['ipessoa'].split(',')
                        for i_pessoas in list_pessoas:
                            new_cpf = geraCfp()
                            dadoAlterado.append(f"Alterado CPF da pessoa física  {i_pessoas} de {row['cpf']} para {new_cpf}")
                            comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set cpf = '{new_cpf}' where i_pessoas = {i_pessoas};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_duplicado: {e}")
            return

        if cpf_duplicado:
            dado = analisa_cpf_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_duplicado(listDados=dado)

    def analisa_corrige_pis_duplicado(pre_validacao):
        nomeValidacao = "PIS duplicado"

        def analisa_pis_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pis_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        list(pf.i_pessoas) ipessoa,
                        num_pis,
                        count(num_pis) AS quantidade
                    FROM 
                        bethadba.pessoas_fisicas pf 
                    GROUP BY 
                        num_pis 
                    HAVING 
                        quantidade > 1   
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        list_pessoas = row['ipessoa'].split(',')
                        for i_pessoas in list_pessoas:
                            new_pis = geraPis()
                            dadoAlterado.append(f"Alterado PIS da pessoa física  {i_pessoas} de {row['num_pis']} para {new_pis}")
                            comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set num_pis = '{new_pis}' where i_pessoas = {i_pessoas};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pis_duplicado: {e}")
            return

        if pis_duplicado:
            dado = analisa_pis_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_pis_duplicado(listDados=dado)

    def analisa_corrige_pis_invalido(pre_validacao):
        nomeValidacao = "A pessoa possui PIS inválido"

        def analisa_pis_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pis_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        p.tipo_pessoa,
                        p.i_pessoas,
                        pf.num_pis
                    FROM bethadba.pessoas p
                    LEFT JOIN bethadba.pessoas_fisicas pf on pf.i_pessoas = p.i_pessoas 
                    WHERE 
                        bethadba.dbf_chk_pis(pf.num_pis) > 0
                        and p.tipo_pessoa != 'J'
                        and pf.num_pis is not null    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        new_pis = geraPis()
                        dadoAlterado.append(f"Alterado PIS da pessoa {row['i_pessoas']} de {row['num_pis']} para {new_pis}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set num_pis = '{new_pis}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pis_invalido: {e}")
            return

        if pis_invalido:
            dado = analisa_pis_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_pis_invalido(listDados=dado)

    def analisa_corrige_rg_duplicado(pre_validacao):
        nomeValidacao = "Pessoa Física com RG duplicado"

        def analisa_rg_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_rg_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        list(i_pessoas) as pess,
                        rg,
                        count(rg) AS quantidade
                    FROM 
                        bethadba.pessoas_fisicas 
                    GROUP BY 
                        rg 
                    HAVING 
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        list_pessoas = row['pess'].split(',')
                        for i_pessoas in list_pessoas[1:]:
                            new_rg = generateRG()
                            dadoAlterado.append(f"Alterado RG da pessoa física  {i_pessoas} de {row['rg']} para {new_rg}")
                            comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set rg = '{new_rg}' where i_pessoas = {i_pessoas};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_rg_duplicado: {e}")
            return

        if rg_duplicado:
            dado = analisa_rg_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_rg_duplicado(listDados=dado)

    def analisa_corrige_data_nascimento_nulo(pre_validacao):
        nomeValidacao = "Pessoa física com data de nascimento nulo."

        def analisa_data_nascimento_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_nascimento_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select  
                        p.i_pessoas,
                        min(f.dt_admissao) as dt_adm,
                        case 
                            when min(f.dt_admissao) is null then dateformat(today() - 18*365,'yyyy-MM-dd')
                            else dateformat(min(f.dt_admissao) - 18 * 365,'yyyy-MM-dd')
                        end as nova_data	
                    from bethadba.pessoas p 
                    join bethadba.pessoas_fisicas pf on pf.i_pessoas = p.i_pessoas
                    join bethadba.funcionarios f on f.i_pessoas = pf.i_pessoas 
                    where pf.dt_nascimento is null
                    GROUP by p.i_pessoas    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de nascimento da pessoa fisica {row['i_pessoas']} de nulo para {row['nova_data']}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set dt_nascimento = '{row['nova_data']}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_nascimento_nulo: {e}")
            return

        if data_nascimento_nulo:
            dado = analisa_data_nascimento_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_data_nascimento_nulo(listDados=dado)

    def analisa_corrige_filiacao_duplicada(pre_validacao):
        nomeValidacao = "Pessoa fisica com afiliação duplicada."

        def analisa_filiacao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_filiacao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_pessoas,
                        nome_pai,
                        nome_mae 
                    from bethadba.pessoas_fis_compl 
                    where nome_pai = nome_mae 
                    and nome_pai is not null 
                    and nome_pai <> ''    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        new_name = row['nome_pai'] + ' | 1' if len(row['nome_pai'] + ' | 1') <= 50 else row['nome_pai'][:50 - 4] + ' | 1'
                        dadoAlterado.append(f"Alterado nome do pai da pessoa {row['i_pessoas']} de {row['nome_pai']} para {new_name}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fis_compl set nome_pai = '{new_name}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_filiacao_duplicada: {e}")
            return

        if filiacao_duplicada:
            dado = analisa_filiacao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_filiacao_duplicada(listDados=dado)

    def analisa_corrige_email_invalido(pre_validacao):
        nomeValidacao = "Pessoa fisica com e-mail inválido."

        def analisa_email_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_email_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                if len(dado) > 0:
                    for fields in dado:
                        dadoAlterado.append(f"Alterado email da pessoa {fields['i_chave_dsk1']} de {fields['i_chave_dsk2']} para nulo.")
                        comandoUpdate += f"""UPDATE bethadba.pessoas set email = NULL where i_pessoas = {fields['i_chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_email_invalido: {e}")
            return

        if email_invalido:
            dado = analisa_email_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_email_invalido(listDados=dado)

    def analisa_corrige_dependente_mesma_pessoa_responsavel(pre_validacao):
        nomeValidacao = "Dependente é a mesma pessoa que o responsável."

        def analisa_dependente_mesma_pessoa_responsavel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_mesma_pessoa_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            comandoDelete = ""
            dadoDeletado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        pf.i_pessoas as pessoa,
                        trim(p.nome) as nm,
                        pf.cpf as cpf, 
                        dep.i_pessoas as pessoa_dep, 
                        trim(pdep.nome) nm_dep, 
                        coalesce(dep.cpf,'00000000000') cpf_dep
                    from bethadba.dependentes d, bethadba.pessoas_fisicas pf, bethadba.pessoas p, bethadba.pessoas_fisicas dep, bethadba.pessoas pdep
                    where pf.i_pessoas = d.i_pessoas
                    and pf.i_pessoas = p.i_pessoas
                    and dep.i_pessoas = d.i_dependentes
                    and dep.i_pessoas = pdep.i_pessoas
                    and ((dep.cpf = pf.cpf) or (pdep.nome = p.nome))  
                 """)

                if len(busca) > 0:
                    for field in busca:
                        if field['pessoa_dep'] == field['pessoa']:
                            dadoDeletado.append(f"Deletado cadastro do dependente {field['pessoa_dep']} pessoa {field['pessoa']} por serem os mesmos.")
                            comandoDelete += f"""DELETE FROM bethadba.dependentes where i_pessoas = {field['pessoa']} and i_dependentes = {field['pessoa_dep']};\n"""
                        else:
                            novo_nome = field['nm_dep'] + ' (dep)' if len(field['nm_dep'] + ' (dep)') <= 50 else field['nm_dep'][:50 -6] + ' (dep)'
                            dadoAlterado.append(f"Alterado nome do dependente {field['pessoa_dep']} - {field['nm_dep']} para {novo_nome}")
                            comandoUpdate += f"""UPDATE bethadba.pessoas set nome = '{novo_nome}' where i_pessoas = {field['pessoa_dep']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao="EXCLUSAO", nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoDeletado)

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_mesma_pessoa_responsavel: {e}")
            return

        if dependente_mesma_pessoa_responsavel:
            dado = analisa_dependente_mesma_pessoa_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_mesma_pessoa_responsavel(listDados=dado)

    if dadosList:
        analisa_corrige_cpf_duplicado(pre_validacao="cpf_duplicado")
        analisa_corrige_pis_duplicado(pre_validacao="pis_duplicado")
        analisa_corrige_pis_invalido(pre_validacao="pis_invalido")
        analisa_corrige_rg_duplicado(pre_validacao="rg_duplicado")
        analisa_corrige_data_nascimento_nulo(pre_validacao="data_nascimento_nulo")
        analisa_corrige_filiacao_duplicada(pre_validacao="filiacao_duplicada")
        analisa_corrige_email_invalido(pre_validacao="email_invalido")
        analisa_corrige_dependente_mesma_pessoa_responsavel(pre_validacao="dependente_mesma_pessoa_responsavel")
