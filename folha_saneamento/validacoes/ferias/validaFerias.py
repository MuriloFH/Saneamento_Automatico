from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def ferias(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
           corrigirErros=False,
           folha_ferias_sem_data_pagamento=False,
           cancelamento_ferias_sem_tipo_afastamento=False,
           data_inicial_afastamento_maior_data_final=False,
           ferias_dt_gozo_ini_menor_dt_admissao=False,
           motivo_cancelamento_maior_50_caracteres=False,
           ferias_concomitantes=False
           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_folha_ferias_sem_data_pagamento(pre_validacao):
        nomeValidacao = "Folha sem data de fechamento."

        def analisa_folha_ferias_sem_data_pagamento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_folha_ferias_sem_data_pagamento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select *
                    from (
                        select 
                            ROW_NUMBER() over(partition by aux.i_ferias, aux.i_funcionarios, aux.i_entidades order by aux.i_entidades, aux.i_funcionarios, aux.i_ferias) as contador,
                            aux.*
                        from (	
                            select 
                            p.i_entidades, p.i_tipos_proc, p.i_competencias, dc.i_funcionarios, dc.dt_pagto as dt_pagamento_dc, p.i_processamentos, p.dt_pagto as dt_pagamento_proc, fp.i_ferias, f.i_periodos, date(left(f.dt_gozo_ini,7)||'-01') as nova_data_pgto
                            from bethadba.dados_calc dc 
                            left join bethadba.processamentos p on (dc.i_entidades = p.i_entidades and dc.i_tipos_proc = p.i_tipos_proc and dc.i_competencias = p.i_competencias and dc.i_processamentos = p.i_processamentos)
                            left join bethadba.ferias_proc as fp on (fp.i_entidades = dc.i_entidades and fp.i_funcionarios = dc.i_funcionarios and fp.i_tipos_proc = dc.i_tipos_proc and fp.i_competencias = dc.i_competencias and fp.i_processamentos = dc.i_processamentos)
                            left join bethadba.ferias as f on f.i_entidades = fp.i_entidades and f.i_funcionarios = fp.i_funcionarios and f.i_ferias = fp.i_ferias
                            where p.i_tipos_proc = 80 
                            and (dc.dt_pagto is null or dc.dt_pagto <= '1900-01-01')
                            and p.dt_pagto  is null or p.dt_pagto <= '1900-01-01')aux
                        ) cte 
                    where cte.contador = 1	and cte.dt_pagamento_proc is null
                    order by cte.i_entidades, cte.i_funcionarios, cte.i_ferias  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        # Ajusta a data na tabela processamentos e dados_calc
                        dadoAlterado.append(
                            f"Alterada data de pagamento da folha de férias: Competencia {row['i_competencias']}, tipo processamento {row['i_tipos_proc']}, processamento {row['i_processamentos']}, funcionario {row['i_funcionarios']} entidade {row['i_entidades']} para {row['nova_data_pgto']}")
                        comandoUpdate += f"""UPDATE bethadba.processamentos set dt_pagto = '{row['nova_data_pgto']}' where i_entidades = {row['i_entidades']} and i_competencias = '{row['i_competencias']}' and i_tipos_proc = {row['i_tipos_proc']} and i_processamentos = {row['i_processamentos']} and (dt_pagto is null or dt_pagto <= '1900-01-01');\n"""
                        comandoUpdate += f"""UPDATE bethadba.dados_calc set dt_pagto = '{row['nova_data_pgto']}' where i_entidades = {row['i_entidades']} and i_competencias = '{row['i_competencias']}' and i_tipos_proc = {row['i_tipos_proc']} and i_funcionarios = {row['i_funcionarios']} and (dt_pagto is null or dt_pagto <= '1900-01-01');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_folha_ferias_sem_data_pagamento: {e}")
            return

        if folha_ferias_sem_data_pagamento:
            dado = analisa_folha_ferias_sem_data_pagamento()

            if corrigirErros and len(dado) > 0:
                corrige_folha_ferias_sem_data_pagamento(listDados=dado)

    def analisa_corrige_cancelamento_ferias_sem_tipo_afastamento(pre_validacao):
        nomeValidacao = "Cancelamento de férias sem tipo de afastamento vinculado"

        def analisa_cancelamento_ferias_sem_tipo_afastamento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cancelamento_ferias_sem_tipo_afastamento(listDados):
            tipoCorrecao = "INSERSÃO"
            comandoInsert = ""
            dadoInserido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""
                       SELECT cf.i_canc_ferias, cf.descricao, (SELECT top 1 i_tipos_afast FROM bethadba.tipos_afast order by i_tipos_afast ) as i_tipos_afast
                        FROM bethadba.canc_ferias_afast cfa    
                        right join bethadba.canc_ferias cf on cf.i_canc_ferias = cfa.i_canc_ferias
                        where cfa.i_tipos_afast is null
                    """)

                if len(busca) > 0:
                    for row in busca:
                        dadoInserido.append(f"Inserido vinculo de cancelamento de ferias {row['i_canc_ferias']}-{row['descricao']} com tipo de afastamento: {row['i_tipos_afast']}")
                        comandoInsert += f"""INSERT INTO bethadba.canc_ferias_afast (i_canc_ferias,i_tipos_afast) values({row['i_canc_ferias']}, {row['i_tipos_afast']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cancelamento_ferias_sem_tipo_afastamento: {e}")
            return

        if cancelamento_ferias_sem_tipo_afastamento:
            dado = analisa_cancelamento_ferias_sem_tipo_afastamento()

            if corrigirErros and len(dado) > 0:
                corrige_cancelamento_ferias_sem_tipo_afastamento(listDados=dado)

    def analisa_corrige_data_inicial_afastamento_maior_data_final(pre_validacao):
        nomeValidacao = "Afastamento com data inicial maior que a data final."

        def analisa_data_inicial_afastamento_maior_data_final():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_inicial_afastamento_maior_data_final(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades, 
                        i_funcionarios, 
                        i_ferias,
                        dt_gozo_ini,
                        dt_gozo_fin 
                    FROM 
                        bethadba.ferias 
                    WHERE 
                        dt_gozo_ini > dt_gozo_fin  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data final do afastamento de férias {row['i_ferias']} do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} de {row['dt_gozo_fin']} para {row['dt_gozo_ini']}")
                        comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_ini = '{row['dt_gozo_fin']}', dt_gozo_fin = '{row['dt_gozo_ini']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_ferias = {row['i_ferias']} and dt_gozo_ini = '{row['dt_gozo_ini']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_inicial_afastamento_maior_data_final: {e}")
            return

        if data_inicial_afastamento_maior_data_final:
            dado = analisa_data_inicial_afastamento_maior_data_final()

            if corrigirErros and len(dado) > 0:
                corrige_data_inicial_afastamento_maior_data_final(listDados=dado)

    def analisa_corrige_ferias_dt_gozo_ini_menor_dt_admissao(pre_validacao):
        nomeValidacao = "Férias com data de gozo inicial menor que a data de admissão."

        def analisa_ferias_dt_gozo_ini_menor_dt_admissao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ferias_dt_gozo_ini_menor_dt_admissao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        f.i_entidades,
                        f.i_funcionarios,
                        f.dt_admissao,
                        fer.i_ferias,
                        fer.dt_gozo_ini,    
                        fer.num_dias_abono,
                        fer.saldo_dias
                    from 
                        bethadba.funcionarios f
                        join bethadba.ferias fer on fer.i_funcionarios = f.i_funcionarios and  fer.i_entidades = f.i_entidades
                    where 
                        dt_admissao>dt_gozo_ini 
                        and cast(fer.num_dias_abono as int) < fer.saldo_dias  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de inicio de gozo de férias do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} para {row['dt_admissao']}")
                        comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_ini = '{row['dt_admissao']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_ferias = {row['i_ferias']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ferias_dt_gozo_ini_menor_dt_admissao: {e}")
            return

        if ferias_dt_gozo_ini_menor_dt_admissao:
            dado = analisa_ferias_dt_gozo_ini_menor_dt_admissao()

            if corrigirErros and len(dado) > 0:
                corrige_ferias_dt_gozo_ini_menor_dt_admissao(listDados=dado)

    def analisa_corrige_motivo_cancelamento_maior_50_caracteres(pre_validacao):
        nomeValidacao = "Motivo do cancelamento de férias possui mais de 50 caracteres."

        def analisa_motivo_cancelamento_maior_50_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_motivo_cancelamento_maior_50_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_entidades, 
                        i_funcionarios, 
                        i_periodos, 
                        i_periodos_ferias,
                        dt_periodo,
                        observacao
                    from bethadba.periodos_ferias 
                    where 
                        tipo = 5
                        and length(observacao) > 50 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado motivo de cancelamento {row['i_periodos']} periodo de ferias {row['i_periodos_ferias']} do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} para {row['observacao'][:50]}")
                        comandoUpdate += f"""UPDATE bethadba.periodos_ferias set observacao = '{row['observacao'][:50]}' where i_entidades  = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_periodos  = {row['i_periodos']} and i_periodos_ferias = {row['i_periodos_ferias']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_motivo_cancelamento_maior_50_caracteres: {e}")
            return

        if motivo_cancelamento_maior_50_caracteres:
            dado = analisa_motivo_cancelamento_maior_50_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_motivo_cancelamento_maior_50_caracteres(listDados=dado)

    def analisa_corrige_ferias_concomitantes(pre_validacao):
        nomeValidacao = "Férias concomitantes do mesmo funcionário"

        def analisa_ferias_concomitantes():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ferias_concomitantes(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select 
                                            a.i_entidades,
                                            a.i_funcionarios,
                                            a.i_ferias as iFeriasA,
                                            a.dt_gozo_ini inicialFeriasA,
                                            a.dt_gozo_fin finalFeriasA,
                                            a.num_dias_dir as numDiasDireitoA,
                                            b.i_ferias as iFeriasB,
                                            b.dt_gozo_ini inicialFeriasB,
                                            b.dt_gozo_fin finalFeriasB,
                                            b.num_dias_dir as numDiasDireitoB
                                            from bethadba.ferias a 
                                            join bethadba.ferias b on a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios 
                                            where (a.dt_gozo_ini  BETWEEN b.dt_gozo_ini  and b.dt_gozo_fin
                                            or a.dt_gozo_fin BETWEEN b.dt_gozo_ini and b.dt_gozo_fin)
                                            and (a.dt_gozo_ini  <> b.dt_gozo_ini  or a.dt_gozo_fin  <> b.dt_gozo_fin)
                                        """)
                entAtual = None
                funcAtual = None
                for row in dados:
                    if entAtual != row['i_entidades']:
                        entAtual = row['i_entidades']
                        if funcAtual != row['i_funcionarios']:
                            dtInicial = ""
                            dtFinal = ""
                            funcAtual = row['i_funcionarios']

                            feriasFunc = banco.consultar(f"""SELECT i_ferias, dt_gozo_ini, dt_gozo_fin, num_dias_dir
                                                                from bethadba.ferias f 
                                                                where f.i_entidades = {entAtual} and f.i_funcionarios = {funcAtual}
                                                                order by i_ferias asc;
                                                        """)

                            for i in range(len(feriasFunc)):
                                diasDireito = feriasFunc[i]['num_dias_dir']

                                # considerando a data inicial da primeira férias como a correta
                                if i == 0:
                                    dtInicial = feriasFunc[i]['dt_gozo_ini']

                                    dtFinal = dtInicial
                                    dtFinal += datetime.timedelta(days=diasDireito)

                                else:
                                    dtInicial = dtFinal
                                    dtInicial += datetime.timedelta(days=365)

                                    dtFinal = dtInicial
                                    dtFinal += datetime.timedelta(days=diasDireito)

                                dadoAlterado.append(f"Alterado a data incial para {dtInicial} e final para {dtFinal} das férias {feriasFunc[i]['i_ferias']} do funcionário {funcAtual} da entidade {entAtual}")
                                comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_ini = '{dtInicial}' where i_entidades = {entAtual} and i_funcionarios = {funcAtual} and i_ferias = {feriasFunc[i]['i_ferias']};\n"""
                                comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_fin = '{dtFinal}' where i_entidades = {entAtual} and i_funcionarios = {funcAtual} and i_ferias = {feriasFunc[i]['i_ferias']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ferias_concomitantes: {e}")
            return

        if ferias_concomitantes:
            dado = analisa_ferias_concomitantes()

            if corrigirErros and len(dado) > 0:
                corrige_ferias_concomitantes(listDados=dado)

    if dadosList:
        analisa_corrige_folha_ferias_sem_data_pagamento(pre_validacao="folha_ferias_sem_data_pagamento")
        analisa_corrige_cancelamento_ferias_sem_tipo_afastamento(pre_validacao="cancelamento_ferias_sem_tipo_afastamento")
        analisa_corrige_data_inicial_afastamento_maior_data_final(pre_validacao="data_inicial_afastamento_maior_data_final")
        analisa_corrige_ferias_dt_gozo_ini_menor_dt_admissao(pre_validacao="ferias_dt_gozo_ini_menor_dt_admissao")
        analisa_corrige_motivo_cancelamento_maior_50_caracteres(pre_validacao="motivo_cancelamento_maior_50_caracteres")
        analisa_corrige_ferias_concomitantes(pre_validacao='ferias_concomitantes')
