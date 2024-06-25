from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl
import datetime


def pessoas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
            corrigirErros=False,
            data_nascimento_maior_data_admissao=False,
            data_vencimento_cnh_menor_data_emissao_1_habilitacao=False,
            data_nascimento_maior_data_dependencia=False,
            num_certidao_maior_32_caracter=False,
            dt_nascimento_maior_dt_chegada=False,
            pf_sem_historico=False
            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_nascimento_maior_data_admissao(pre_validacao):
        nomeValidacao = "A pessoa possui data de nascimento maior que data de admissão"

        def analisa_data_nascimento_maior_data_admissao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_nascimento_maior_data_admissao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_funcionarios,
                        i_entidades,
                        f.dt_admissao,
                        pf.dt_nascimento,
                        year(f.dt_admissao) - 18 || right(f.dt_admissao,6) as nova_dt_nascimento,
                        pf.i_pessoas,
                        p.nome
                    FROM 
                        bethadba.funcionarios f
                    INNER JOIN 
                        bethadba.pessoas_fisicas pf ON (f.i_pessoas = pf.i_pessoas)
                    INNER JOIN 
                        bethadba.pessoas p ON (f.i_pessoas = p.i_pessoas)
                    WHERE
                        pf.dt_nascimento > f.dt_admissao    
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de nascimento da pessoa {row['i_pessoas']} - {row['nome']} para {row['nova_dt_nascimento']}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set dt_nascimento = '{row['nova_dt_nascimento']}' where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_nascimento_maior_data_admissao: {e}")
            return

        if data_nascimento_maior_data_admissao:
            dado = analisa_data_nascimento_maior_data_admissao()

            if corrigirErros and len(dado) > 0:
                corrige_data_nascimento_maior_data_admissao(listDados=dado)

    def analisa_corrige_data_vencimento_cnh_menor_data_emissao_1_habilitacao(pre_validacao):
        nomeValidacao = "A pessoa possui data de vencimento da CNH menor que a data de emissão da 1ª habilitação"

        def analisa_data_vencimento_cnh_menor_data_emissao_1_habilitacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_vencimento_cnh_menor_data_emissao_1_habilitacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select i_pessoas,dt_primeira_cnh,dt_vencto_cnh, dateformat(dt_primeira_cnh+1,'yyyy-mm-dd') as nova_dt_vencimento, 'pessoas_fis_compl' as tabela
                    from bethadba.pessoas_fis_compl
                    where dt_primeira_cnh > dt_vencto_cnh
                    union
                    select i_pessoas,dt_primeira_cnh,dt_vencto_cnh, dateformat(dt_primeira_cnh+1,'yyyy-mm-dd') as nova_dt_vencimento, 'hist_pessoas_fis' as tabela
                    from bethadba.hist_pessoas_fis hpf  
                    where dt_primeira_cnh > dt_vencto_cnh    
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de vencimento da CNH da pessoa {row['i_pessoas']} de {row['dt_vencto_cnh']} para {row['nova_dt_vencimento']} tabela {row['tabela']}")
                        comandoUpdate += f"""UPDATE bethadba.{row['tabela']} set dt_vencto_cnh = '{row['nova_dt_vencimento']}' where i_pessoas = {row['i_pessoas']};\n"""
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_vencimento_cnh_menor_data_emissao_1_habilitacao: {e}")
            return

        if data_vencimento_cnh_menor_data_emissao_1_habilitacao:
            dado = analisa_data_vencimento_cnh_menor_data_emissao_1_habilitacao()

            if corrigirErros and len(dado) > 0:
                corrige_data_vencimento_cnh_menor_data_emissao_1_habilitacao(listDados=dado)

    def analisa_corrige_data_nascimento_maior_data_dependencia(pre_validacao):
        nomeValidacao = "Pessoas com data de nascimento maior que data de dependencia"

        def analisa_data_nascimento_maior_data_dependencia():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_nascimento_maior_data_dependencia(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        d.i_dependentes,
                        pf.dt_nascimento,
                        d.dt_ini_depende 
                    FROM 
                        bethadba.dependentes d 
                    JOIN 
                        bethadba.pessoas_fisicas pf  ON (d.i_dependentes = pf.i_pessoas)
                    WHERE 
                        dt_nascimento > dt_ini_depende     
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de inicio dependencia do dependente {row['i_dependentes']} de {row['dt_ini_depende']} para {row['dt_nascimento']}")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set dt_ini_depende = '{row['dt_nascimento']}' where i_dependentes = {row['i_dependentes']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_nascimento_maior_data_dependencia: {e}")
            return

        if data_nascimento_maior_data_dependencia:
            dado = analisa_data_nascimento_maior_data_dependencia()

            if corrigirErros and len(dado) > 0:
                corrige_data_nascimento_maior_data_dependencia(listDados=dado)

    def analisa_corrige_num_certidao_maior_32_caracter(pre_validacao):
        nomeValidacao = "Numero da certidão maior que 32 caracter"

        def analisa_num_certidao_maior_32_caracter():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_num_certidao_maior_32_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select 
                                            i_pessoas as pessoas,
                                            num_reg as numeroCertidao
                                            from bethadba.pessoas_fis_compl pfc
                                           where LENGTH(num_reg) > 32 
                                        """)

                for row in dados:
                    newNumeroCertidao = row['numeroCertidao'][:32]

                    dadoAlterado.append(f"""Alterado o numero da certidão da pessoa {row['pessoas']} para {newNumeroCertidao}""")
                    comandoUpdate += f"""UPDATE bethadba.pessoas_fis_compl set num_reg = '{newNumeroCertidao}' where i_pessoas = {row['pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_num_certidao_maior_32_caracter: {e}")
            return

        if num_certidao_maior_32_caracter:
            dado = analisa_num_certidao_maior_32_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_num_certidao_maior_32_caracter(listDados=dado)

    def analisa_corrige_dt_nascimento_maior_dt_chegada(pre_validacao):
        nomeValidacao = "Pessoas estrangeiras com data de nascimento maior que data de chegada"

        def analisa_dt_nascimento_maior_dt_chegada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_nascimento_maior_dt_chegada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select pf.i_pessoas,
                                            pf.dt_nascimento,
                                            pe.data_chegada  
                                            from bethadba.pessoas_fisicas pf 
                                            join bethadba.pessoas_estrangeiras pe on pf.i_pessoas = pe.i_pessoas 
                                            where pf.dt_nascimento > pe.data_chegada
                                        """)

                for row in dados:
                    newDtNascimento = row['data_chegada']
                    newDtNascimento -= datetime.timedelta(days=6574.5)

                    dadoAlterado.append(f"Alterado a data de nascimento para {newDtNascimento} da pessoa {row['i_pessoas']}")
                    comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set dt_nascimento = '{newDtNascimento}' and i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_nascimento_maior_dt_chegada: {e}")
            return

        if dt_nascimento_maior_dt_chegada:
            dado = analisa_dt_nascimento_maior_dt_chegada()

            if corrigirErros and len(dado) > 0:
                corrige_dt_nascimento_maior_dt_chegada(listDados=dado)

    def analisa_corrige_pf_sem_historico(pre_validacao):
        nomeValidacao = "Pessoa física sem histórico"

        def analisa_pf_sem_historico():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pf_sem_historico(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select
                                                pf.i_pessoas as pessoa,
                                                pf.dt_nascimento as dtAlteracoes,
                                                tipoPessoa = 'F'
                                            from bethadba.pessoas_fisicas pf
                                            where i_pessoas not in (select distinct(i_pessoas) from bethadba.hist_pessoas_fis hpf)
                                        """)
                for row in dados:
                    dtAlteracao = row['dtAlteracoes']
                    dtAlteracao += datetime.timedelta(days=6.574)

                    dadoAlterado.append(f"Inserido um histórico com data de {dtAlteracao} para a pessoa {row['pessoa']}")
                    comandoInsert += f"""INSERT into bethadba.hist_pessoas_fis(i_pessoas, dt_nascimento, tipo_pessoa)
                                        values({row['pessoa']}, '{row['dtAlteracoes']}', '{row['tipoPessoa']}');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pf_sem_historico: {e}")
            return

        if pf_sem_historico:
            dado = analisa_pf_sem_historico()

            if corrigirErros and len(dado) > 0:
                corrige_pf_sem_historico(listDados=dado)

    if dadosList:
        analisa_corrige_data_nascimento_maior_data_admissao(pre_validacao="data_nascimento_maior_data_admissao")
        analisa_corrige_data_vencimento_cnh_menor_data_emissao_1_habilitacao(pre_validacao="data_vencimento_cnh_menor_data_emissao_1_habilitacao")
        analisa_corrige_data_nascimento_maior_data_dependencia(pre_validacao="data_nascimento_maior_data_dependencia")
        analisa_corrige_num_certidao_maior_32_caracter(pre_validacao='num_certidao_maior_32_caracter')
        analisa_corrige_dt_nascimento_maior_dt_chegada(pre_validacao="dt_nascimento_maior_dt_chegada")
        analisa_corrige_pf_sem_historico(pre_validacao="pf_sem_historico")
