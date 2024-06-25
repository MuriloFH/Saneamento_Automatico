from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def dependente(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
               corrigirErros=False,
               dependente_data_nascimento_menor_data_nascimento_responsavel=False,
               dependente_sem_motivo_de_termino=False,
               dependente_data_nascimento_maior_data_nascimento_responsavel=False,
               dependente_data_inicio_menor_data_nascimento=False,
               dependente_dt_casamento_menor_dt_nascimento=False,
               dependente_sem_dt_inicial=False,
               dependente_motivo_inicial_nulo=False,
               dependente_com_mais_de_uma_config_IRRF=False
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dependente_data_nascimento_menor_data_nascimento_responsavel(pre_validacao):
        nomeValidacao = "Dependente grau de parentesco(1-Filho(a)/6-Neto/8-Menor Tutelado/11-Bisneto) com data de nascimento MENOR que a do seu responsável."

        def analisa_dependente_data_nascimento_menor_data_nascimento_responsavel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_data_nascimento_menor_data_nascimento_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        d.i_pessoas as i_pessoas_resp ,
                        p.dt_nascimento as dt_nascimento_resp, 
                        d.i_dependentes as i_dependentes, 
                        pdep.dt_nascimento as dt_nascimento_dep, 
                        d.grau
                    from bethadba.dependentes d
                    join bethadba.pessoas_fisicas p on(p.i_pessoas = d.i_pessoas)
                    join bethadba.pessoas_fisicas pdep on(pdep.i_pessoas = d.i_dependentes)
                    where d.grau in(1,6,8,11)
                    and pdep.dt_nascimento < p.dt_nascimento  
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de nascimento do depentende {row['i_dependentes']} de {row['dt_nascimento_dep']} para {row['dt_nascimento_resp']}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set dt_nascimento = '{row['dt_nascimento_resp']}' where i_pessoas = {row['i_dependentes']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_data_nascimento_menor_data_nascimento_responsavel: {e}")
            return

        if dependente_data_nascimento_menor_data_nascimento_responsavel:
            dado = analisa_dependente_data_nascimento_menor_data_nascimento_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_data_nascimento_menor_data_nascimento_responsavel(listDados=dado)

    def analisa_corrige_dependente_sem_motivo_de_termino(pre_validacao):
        nomeValidacao = "Dependentes sem motivo de término."

        def analisa_dependente_sem_motivo_de_termino():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_sem_motivo_de_termino(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_pessoas ,
                        i_dependentes,
                        dt_ini_depende,
                        mot_fin_depende,
                        dt_fin_depende
                    FROM 
                        bethadba.dependentes d  
                    WHERE 
                        mot_fin_depende IS NULL AND 
                        dt_fin_depende IS NOT NULL  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado motivo do fim da relação de dependência do dependente {row['i_dependentes']} da pessoa  {row['i_pessoas']} para 0")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set mot_fin_depende = 0 where i_dependentes = {row['i_dependentes']} and i_pessoas = {row['i_pessoas']} and dt_ini_depende = '{row['dt_ini_depende']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_sem_motivo_de_termino: {e}")
            return

        if dependente_sem_motivo_de_termino:
            dado = analisa_dependente_sem_motivo_de_termino()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_sem_motivo_de_termino(listDados=dado)

    def analisa_corrige_dependente_data_nascimento_maior_data_nascimento_responsavel(pre_validacao):
        nomeValidacao = "Dependente grau de parentesco(3-Pai/Mãe/4-Avô/Avó/12-Bisavô/Bisavó) com data de nascimento MAIOR que a do seu responsável."

        def analisa_dependente_data_nascimento_maior_data_nascimento_responsavel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_data_nascimento_maior_data_nascimento_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        d.i_pessoas as i_pessoas_resp,
                        p.dt_nascimento, 
                        d.i_dependentes, 
                        pdep.dt_nascimento as dt_nascimento_dep, 
                        p.dt_nascimento as dt_nascimento_resp,
                        d.grau grau_p
                    from bethadba.dependentes d
                    join bethadba.pessoas_fisicas p on(p.i_pessoas = d.i_pessoas)
                    join bethadba.pessoas_fisicas pdep on(pdep.i_pessoas = d.i_dependentes)
                    where d.grau in(3,4,12)
                    and pdep.dt_nascimento > p.dt_nascimento
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de nascimento do depentende {row['i_dependentes']} de {row['dt_nascimento_dep']} para {row['dt_nascimento_resp']}")
                        comandoUpdate += f"""UPDATE bethadba.pessoas_fisicas set dt_nascimento = '{row['dt_nascimento_resp']}' where i_pessoas = {row['i_dependentes']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_data_nascimento_maior_data_nascimento_responsavel: {e}")
            return

        if dependente_data_nascimento_maior_data_nascimento_responsavel:
            dado = analisa_dependente_data_nascimento_maior_data_nascimento_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_data_nascimento_maior_data_nascimento_responsavel(listDados=dado)

    def analisa_corrige_dependente_data_inicio_menor_data_nascimento(pre_validacao):
        nomeValidacao = "Dependente com data de inicio de dependencia menor que a data de nascimento do dependente."

        def analisa_dependente_data_inicio_menor_data_nascimento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_data_inicio_menor_data_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        d.i_pessoas as pessoa, 
                        d.i_dependentes as dep, 
                        d.dt_ini_depende as dt_ini_dep, 
                        pf.dt_nascimento as dt_nasc
                    from bethadba.dependentes d, bethadba.pessoas_fisicas pf
                    where d.i_dependentes = pf.i_pessoas
                    and d.dt_ini_depende is not null
                    and d.dt_ini_depende < pf.dt_nascimento    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado data de inicio dependencia do dependente {row['dep']} de {row['dt_ini_dep']} para {row['dt_nasc']}")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set dt_ini_depende = '{row['dt_nasc']}' where i_dependentes = {row['dep']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_data_inicio_menor_data_nascimento: {e}")
            return

        if dependente_data_inicio_menor_data_nascimento:
            dado = analisa_dependente_data_inicio_menor_data_nascimento()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_data_inicio_menor_data_nascimento(listDados=dado)

    def analisa_corrige_dependente_dt_casamento_menor_dt_nascimento(pre_validacao):
        nomeValidacao = "Dependentes com data de casamento menor que a data de nascimento."

        def analisa_dependente_dt_casamento_menor_dt_nascimento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_dt_casamento_menor_dt_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        d.i_pessoas as pessoa, 
                        d.i_dependentes as dep, 
                        d.dt_casamento as dt_casam, 
                        pf.dt_nascimento as dt_nasc
                    from bethadba.dependentes d, bethadba.pessoas_fisicas pf
                    where d.i_dependentes = pf.i_pessoas
                    and d.dt_casamento is not null
                    and d.dt_casamento < pf.dt_nascimento    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado data de inicio dependencia do dependente {row['dep']} de {row['dt_casam']} para {row['dt_nasc']}")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set dt_casamento = '{row['dt_nasc']}' where i_dependentes = {row['dep']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_dt_casamento_menor_dt_nascimento: {e}")
            return

        if dependente_dt_casamento_menor_dt_nascimento:
            dado = analisa_dependente_dt_casamento_menor_dt_nascimento()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_dt_casamento_menor_dt_nascimento(listDados=dado)

    def analisa_corrige_dependente_sem_dt_inicial(pre_validacao):
        nomeValidacao = "Dependentes sem data inicial informada"

        def analisa_dependente_sem_dt_inicial():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_sem_dt_inicial(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        d.i_pessoas as pessoa, 
                        d.i_dependentes as dep, 
                        d.dt_casamento as dt_casam, 
                        pf.dt_nascimento as dt_nasc
                    from bethadba.dependentes d, bethadba.pessoas_fisicas pf
                    where d.i_dependentes = pf.i_pessoas
                    and d.dt_casamento is not null
                    and d.dt_casamento < pf.dt_nascimento    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado data de inicio dependencia do dependente {row['dep']} de {row['dt_casam']} para {row['dt_nasc']}")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set dt_casamento = '{row['dt_nasc']}' where i_dependentes = {row['dep']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_sem_dt_inicial: {e}")
            return

        if dependente_sem_dt_inicial:
            dado = analisa_dependente_sem_dt_inicial()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_sem_dt_inicial(listDados=dado)

    def analisa_corrige_dependente_motivo_inicial_nulo(pre_validacao):
        nomeValidacao = "Dependentes sem motivo inicial informado"

        def analisa_dependente_motivo_inicial_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_motivo_inicial_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select 
                                            i_pessoas,
                                            i_dependentes,
                                            mot_ini_depende
                                            from bethadba.dependentes d 
                                            where mot_ini_depende is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado o motivo inicial da dependência para 0 para o dependente {row['i_dependentes']}")
                        comandoUpdate += f"""UPDATE bethadba.dependentes set mot_ini_depende = 0 where i_pessoas = {row['i_pessoas']} and i_dependentes = {row['i_dependentes']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_motivo_inicial_nulo: {e}")
            return

        if dependente_motivo_inicial_nulo:
            dado = analisa_dependente_motivo_inicial_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_motivo_inicial_nulo(listDados=dado)

    def analisa_corrige_dependente_com_mais_de_uma_config_IRRF(pre_validacao):
        nomeValidacao = "Dependente com mais de uma configuração de IRRF."

        def analisa_dependente_com_mais_de_uma_config_IRRF():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dependente_com_mais_de_uma_config_IRRF(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                            i_dependentes,
                                            count(i_dependentes) as total
                                            from (select distinct i_dependentes, dep_irrf from bethadba.dependentes_func df) as thd 
                                            group by i_dependentes 
                                            having total > 1
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        listFunc = []
                        listEnt = []
                        dependentes = banco.consultar(f"""SELECT i_funcionarios, i_entidades
                                                            from bethadba.dependentes_func df 
                                                            where df.i_dependentes = {row['i_dependentes']}
                                                            order by i_funcionarios desc
                                                      """)

                        if len(dependentes) > 0:
                            # preservando o primeiro resultado da busca dos dependentes
                            for i in dependentes[1:]:
                                dadoAlterado.append(f"Removido os dependentes do funcionário {i['i_funcionarios']} da entidade {i['i_entidades']}.")
                                comandoDelete += f"""DELETE from bethadba.dependentes_func where i_entidades in ({i['i_entidades']}) and i_funcionarios in ({i['i_funcionarios']}) and i_dependentes = {row['i_dependentes']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dependente_motivo_inicial_nulo: {e}")
            return

        if dependente_com_mais_de_uma_config_IRRF:
            dado = analisa_dependente_com_mais_de_uma_config_IRRF()

            if corrigirErros and len(dado) > 0:
                corrige_dependente_com_mais_de_uma_config_IRRF(listDados=dado)

    if dadosList:
        analisa_corrige_dependente_data_nascimento_menor_data_nascimento_responsavel(pre_validacao="dependente_data_nascimento_menor_data_nascimento_responsavel")
        analisa_corrige_dependente_sem_motivo_de_termino(pre_validacao="dependente_sem_motivo_de_termino")
        analisa_corrige_dependente_data_nascimento_maior_data_nascimento_responsavel(pre_validacao="dependente_data_nascimento_maior_data_nascimento_responsavel")
        analisa_corrige_dependente_data_inicio_menor_data_nascimento(pre_validacao="dependente_data_inicio_menor_data_nascimento")
        analisa_corrige_dependente_dt_casamento_menor_dt_nascimento(pre_validacao="dependente_dt_casamento_menor_dt_nascimento")
        analisa_corrige_dependente_sem_dt_inicial(pre_validacao="dependente_sem_dt_inicial")
        analisa_corrige_dependente_motivo_inicial_nulo(pre_validacao="dependente_motivo_inicial_nulo")
        analisa_corrige_dependente_com_mais_de_uma_config_IRRF(pre_validacao="dependente_com_mais_de_uma_config_IRRF")
