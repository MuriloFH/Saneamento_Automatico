from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def funcionarios(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 alteracao_salario_funcionario_maior_data_rescisao=False,
                 alteracao_cargo_funcionario_maior_data_rescisao=False,
                 funcionario_sem_previdencia=False,
                 data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica=False,
                 funcionario_data_nomeacao_maior_data_posse=False,
                 ferias_dt_gozo_fin_maior_rescisao=False,
                 ferias_gozo_final_apos_rescisao=False,
                 multiplos_locais_trabalho_principal=False,
                 funcionario_sem_local_trabalho_principal=False,
                 dt_fim_lotacao_maior_dt_fim_contrato=False,
                 funcionario_sem_historico=False,
                 funcionario_sem_historico_cargo=False,
                 funcionario_sem_historico_salarial=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_alteracao_salario_funcionario_maior_data_rescisao(pre_validacao):
        nomeValidacao = "Alteração salarial dos funcionários com data maior que a data de rescisão."

        def analisa_alteracao_salario_funcionario_maior_data_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_alteracao_salario_funcionario_maior_data_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        hs.i_funcionarios,
                        hs.i_entidades,
                        hs.dt_alteracoes,
                        r.dt_rescisao,
                        STRING(r.dt_rescisao, ' ', SUBSTRING(hs.dt_alteracoes, 12, 8)) AS dt_alteracoes_novo
                    FROM
                        bethadba.hist_salariais hs
                    INNER JOIN 
                        bethadba.rescisoes r ON (hs.i_funcionarios = r.i_funcionarios AND hs.i_entidades = r.i_entidades)
                    WHERE
                        hs.dt_alteracoes > STRING((select max(s.dt_rescisao) 
                                                   from bethadba.rescisoes s 
                                                   join bethadba.motivos_resc mr on(s.i_motivos_resc = mr.i_motivos_resc)
                                                   where s.i_funcionarios = r.i_funcionarios 
                                                   AND s.i_entidades = r.i_entidades
                                                   and s.dt_canc_resc is null
                                                   and s.dt_reintegracao is null
                                                   and mr.dispensados != 3), ' 23:59:59')
                    ORDER BY 
                        hs.dt_alteracoes DESC   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de alteração salarial do funcionário {row['i_funcionarios']} entidade  {row['i_entidades']} de {row['dt_alteracoes']} para {row['dt_alteracoes_novo']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_salariais set dt_alteracoes = '{row['dt_alteracoes_novo']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_alteracao_salario_funcionario_maior_data_rescisao: {e}")
            return

        if alteracao_salario_funcionario_maior_data_rescisao:
            dado = analisa_alteracao_salario_funcionario_maior_data_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_alteracao_salario_funcionario_maior_data_rescisao(listDados=dado)

    def analisa_corrige_alteracao_cargo_funcionario_maior_data_rescisao(pre_validacao):
        nomeValidacao = "Alteração de cargo dos funcionários com data maior que a data de rescisão."

        def analisa_alteracao_cargo_funcionario_maior_data_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_alteracao_cargo_funcionario_maior_data_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        hs.i_funcionarios,
                        hs.i_entidades,
                        hs.dt_alteracoes,
                        r.dt_rescisao,
                        STRING(r.dt_rescisao, ' ', SUBSTRING(hs.dt_alteracoes, 12, 8)) AS dt_alteracoes_novo
                    FROM
                        bethadba.hist_cargos hs
                    INNER JOIN 
                        bethadba.rescisoes r ON (hs.i_funcionarios = r.i_funcionarios AND hs.i_entidades = r.i_entidades)
                    WHERE
                        hs.dt_alteracoes > STRING((select max(s.dt_rescisao) 
                                                   from bethadba.rescisoes s 
                                                   join bethadba.motivos_resc mr on(s.i_motivos_resc = mr.i_motivos_resc)
                                                   where s.i_funcionarios = r.i_funcionarios 
                                                   AND s.i_entidades = r.i_entidades
                                                   and s.dt_canc_resc is null
                                                   and s.dt_reintegracao is null
                                                   and mr.dispensados != 3), ' 23:59:59')
                    ORDER BY 
                        hs.dt_alteracoes DESC   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de alteração do cargo do funcionário {row['i_funcionarios']} entidade  {row['i_entidades']} de {row['dt_alteracoes']} para {row['dt_alteracoes_novo']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_cargos set dt_alteracoes = '{row['dt_alteracoes_novo']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_alteracao_cargo_funcionario_maior_data_rescisao: {e}")
            return

        if alteracao_cargo_funcionario_maior_data_rescisao:
            dado = analisa_alteracao_cargo_funcionario_maior_data_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_alteracao_cargo_funcionario_maior_data_rescisao(listDados=dado)

    def analisa_corrige_funcionario_sem_previdencia(pre_validacao):
        nomeValidacao = "Funcionário sem previdência"

        def analisa_funcionario_sem_previdencia():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_sem_previdencia(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        hf.i_entidades ,
                        hf.i_funcionarios,
                        hf.dt_alteracoes
                    FROM 
                        bethadba.hist_funcionarios hf
                    INNER JOIN 
                        bethadba.funcionarios f ON f.i_funcionarios = hf.i_funcionarios AND f.i_entidades = hf.i_entidades
                    INNER JOIN 
                        bethadba.rescisoes r ON r.i_funcionarios = hf.i_funcionarios and r.i_entidades = hf.i_entidades
                    INNER JOIN 
                        bethadba.vinculos v ON v.i_vinculos = hf.i_vinculos
                    WHERE
                        hf.prev_federal = 'N' AND
                        hf.prev_estadual = 'N' AND
                        hf.fundo_ass = 'N' AND
                        hf.fundo_prev = 'N' AND
                        hf.fundo_financ = 'N' AND
                        f.tipo_func = 'F' AND 
                        r.i_motivos_resc not in (8) AND
                        v.categoria_esocial <> '901'
                    GROUP BY
                        hf.i_funcionarios,
                        hf.i_entidades,
                        hf.dt_alteracoes
                    ORDER BY
                       hf.i_entidades, hf.i_funcionarios, hf.dt_alteracoes  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado histórico do funcionario {row['i_funcionarios']} data de alteracao {row['dt_alteracoes']} entidade {row['i_entidades']} campo prev_federal para S")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set prev_federal = 'S' where i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}' and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_sem_previdencia: {e}")
            return

        if funcionario_sem_previdencia:
            dado = analisa_funcionario_sem_previdencia()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_sem_previdencia(listDados=dado)

    def analisa_corrige_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica(pre_validacao):
        nomeValidacao = "Data de admissão da matrícula posterior a data de início da matrícula nesta lotação física"

        def analisa_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        f.dt_admissao,
                        lm.i_funcionarios,
                        lm.dt_inicial,
                        lm.i_entidades,
                        lm.i_locais_trab,
                        lm.dt_final 
                    FROM
                        bethadba.funcionarios f
                    INNER JOIN
                        bethadba.locais_mov lm ON (f.i_funcionarios = lm.i_funcionarios AND f.i_entidades = lm.i_entidades)
                    WHERE 
                        f.dt_admissao > lm.dt_inicial   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de inicio na lotacao física {row['i_locais_trab']} do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} para {row['dt_admissao']}")
                        comandoUpdate += f"""UPDATE bethadba.locais_mov set dt_inicial = '{row['dt_admissao']}' where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and i_locais_trab = {row['i_locais_trab']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica: {e}")
            return

        if data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica:
            dado = analisa_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica()

            if corrigirErros and len(dado) > 0:
                corrige_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica(listDados=dado)

    def analisa_corrige_funcionario_data_nomeacao_maior_data_posse(pre_validacao):
        nomeValidacao = "Funcionário com data de nomeação maior que a data de posse."

        def analisa_funcionario_data_nomeacao_maior_data_posse():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_data_nomeacao_maior_data_posse(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_funcionarios, i_entidades, dt_alteracoes , dt_nomeacao , dt_posse
                    FROM 
                        bethadba.hist_cargos
                    WHERE
                        dt_nomeacao > dt_posse
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de nomeacao do histórico de cargo do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} data de alteracao {row['dt_alteracoes']} de {row['dt_nomeacao']} para {row['dt_posse']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_cargos set dt_nomeacao = '{row['dt_posse']}' where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}' and dt_nomeacao = '{row['dt_nomeacao']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_data_nomeacao_maior_data_posse: {e}")
            return

        if funcionario_data_nomeacao_maior_data_posse:
            dado = analisa_funcionario_data_nomeacao_maior_data_posse()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_data_nomeacao_maior_data_posse(listDados=dado)

    def analisa_corrige_ferias_dt_gozo_fin_maior_rescisao(pre_validacao):
        nomeValidacao = "Funcionário possui férias com data de fim do gozo igual ou após a data da rescisão"

        def analisa_ferias_dt_gozo_fin_maior_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ferias_dt_gozo_fin_maior_rescisao(listDados):
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
                        f.i_ferias,
                        f.i_periodos,
                        f.dt_gozo_fin, 
                        r.dt_rescisao,
                        dateformat(dt_rescisao - 1,'yyyy-MM-dd') as novo_gozo_ini
                    from bethadba.ferias f
                    join bethadba.rescisoes r on r.i_entidades = f.i_entidades and r.i_funcionarios = f.i_funcionarios
                    where f.dt_gozo_fin >= r.dt_rescisao
                    and r.dt_canc_resc is null  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de gozo final de ferias do funcionario  {row['i_funcionarios']} entidade {row['i_entidades']} para {row['novo_gozo_ini']}")
                        comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_fin = '{row['novo_gozo_ini']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_ferias = {row['i_ferias']} and i_periodos = {row['i_periodos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ferias_dt_gozo_fin_maior_rescisao: {e}")
            return

        if ferias_dt_gozo_fin_maior_rescisao:
            dado = analisa_ferias_dt_gozo_fin_maior_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_ferias_dt_gozo_fin_maior_rescisao(listDados=dado)

    def analisa_corrige_ferias_gozo_final_apos_rescisao(pre_validacao):
        nomeValidacao = "Férias com gozo final após a rescisão."

        def analisa_ferias_gozo_final_apos_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ferias_gozo_final_apos_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        r.i_entidades, 
                        r.i_funcionarios, 
                        f.i_ferias,
                        f.i_periodos,
                        r.dt_rescisao, 
                        f.dt_gozo_ini, 
                        f.dt_gozo_fin,
                        dateformat(r.dt_rescisao - 1, 'yyyy-MM-dd')  as novo_gozo_fin
                    from bethadba.rescisoes r join bethadba.ferias f on(f.i_entidades = r.i_entidades and f.i_funcionarios = r.i_funcionarios)
                    where r.dt_canc_resc is null
                    and r.dt_rescisao >= f.dt_gozo_ini
                    and r.dt_rescisao <= f.dt_gozo_fin  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de gozo final de ferias do funcionario  {row['i_funcionarios']} entidade {row['i_entidades']} para {row['novo_gozo_fin']}")
                        comandoUpdate += f"""UPDATE bethadba.ferias set dt_gozo_fin = '{row['novo_gozo_fin']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_ferias = {row['i_ferias']} and i_periodos = {row['i_periodos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ferias_gozo_final_apos_rescisao: {e}")
            return

        if ferias_gozo_final_apos_rescisao:
            dado = analisa_ferias_gozo_final_apos_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_ferias_gozo_final_apos_rescisao(listDados=dado)

    def analisa_corrige_multiplos_locais_trabalho_principal(pre_validacao):
        nomeValidacao = "Funcionário com mais de um local de trabalho definido como principal"

        def analisa_multiplos_locais_trabalho_principal():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_multiplos_locais_trabalho_principal(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select 
                                               principal = if isnull(locais_mov.principal,'N') = 'S' then 'true' else 'false' endif,
                                               dataInicio = dt_inicial,
                                               dataFim = dt_final,
                                               i_funcionarios func,
                                               list(i_entidades) as entidade,
                                               list(i_locais_trab) as locais,
                                               count(principal) as total
                                              from bethadba.locais_mov
                                              where i_funcionarios = i_funcionarios 
                                              and i_entidades = i_entidades
                                              and principal = 'true'
                                              group by  principal, dataInicio, dataFim, principal, i_funcionarios
                                              HAVING total > 1
                                              order by locais_mov.i_funcionarios
                                        """)

                for row in dados:
                    print(row)
                    listLocais = row['locais'].split(',')
                    entidades = row['entidade'].split(',')[0]

                    # preservando o primeiro local que retorna do SQL de cada funcionário
                    listLocais = ','.join(listLocais[1:])

                    dadoAlterado.append(f"Alterado para principal 'N' dos locais de trabalho {listLocais} do funcionário {row['func']} da entidade = {entidades}")
                    comandoUpdate += f"""UPDATE bethadba.locais_mov set principal = 'N' where i_funcionarios = {row['func']} and i_entidades = {entidades} and i_locais_trab in ({listLocais});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_multiplos_locais_trabalho_principal: {e}")
            return

        if multiplos_locais_trabalho_principal:
            dado = analisa_multiplos_locais_trabalho_principal()

            if corrigirErros and len(dado) > 0:
                corrige_multiplos_locais_trabalho_principal(listDados=dado)

    def analisa_corrige_funcionario_sem_local_trabalho_principal(pre_validacao):
        nomeValidacao = "Funcionário sem um local de trabalho definido como principal"

        def analisa_funcionario_sem_local_trabalho_principal():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_sem_local_trabalho_principal(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select 
                                                   principal = if isnull(locais_mov.principal,'N') = 'S' then 'true' else 'false' endif,
                                                   list(i_entidades) as entidade,
                                                   i_funcionarios func,
                                                   list(i_locais_trab) as locais,
                                                   count(principal) as total
                                              from bethadba.locais_mov
                                              where i_funcionarios = i_funcionarios 
                                              and i_entidades = i_entidades
                                              and principal = 'false'
                                              and i_funcionarios not in(
                                                                      select lm.i_funcionarios      
                                                                      from bethadba.locais_mov lm
                                                                      where lm.i_funcionarios = locais_mov.i_funcionarios 
                                                                      and lm.i_entidades = locais_mov.i_entidades
                                                                      and lm.principal = 'S'                          
                                                                        )
                                              group by principal, i_funcionarios
                                              HAVING total > 1
                                              order by locais_mov.i_funcionarios
                                        """)

                for row in dados:
                    listLocais = row['locais'].split(',')
                    entidades = row['entidade'].split(',')

                    for i in range(0, len(entidades)):
                        if i > 0 and entidades[i] == entidades[i - 1]:
                            continue
                        else:
                            dadoAlterado.append(f"Alterado para principal 'S' dos locais de trabalho {listLocais[0]} do funcionário {row['func']} da entidade = {entidades[i]}")
                            comandoUpdate += f"""UPDATE bethadba.locais_mov set principal = 'S' where i_funcionarios = {row['func']} and i_entidades = {entidades[i]} and i_locais_trab in ({listLocais[0]});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_multiplos_locais_trabalho_principal: {e}")
            return

        if funcionario_sem_local_trabalho_principal:
            dado = analisa_funcionario_sem_local_trabalho_principal()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_sem_local_trabalho_principal(listDados=dado)

    def analisa_corrige_dt_fim_lotacao_maior_dt_fim_contrato(pre_validacao):
        nomeValidacao = "Data final da lotacao final maior que data fim do contrato"

        def analisa_dt_fim_lotacao_maior_dt_fim_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_fim_lotacao_maior_dt_fim_contrato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select
                                                f.i_entidades,
                                                f.i_funcionarios,
                                                f.dt_inicial,
                                                f.dt_final,
                                                (select max(fv.dt_fim_vinculo) from bethadba.funcionarios_vinctemp fv where fv.i_entidades = f.i_entidades and fv.i_funcionarios = f.i_funcionarios) as dtFimVinculo
                                             from bethadba.locais_mov f
                                             where dtFimVinculo is not NULL
                                             and f.dt_final > dtFimVinculo;
                                        """)

                for row in dados:
                    newDtfinal = row['dtFimVinculo']

                    dadoAlterado.append(f"Alterado a data final para {newDtfinal} da movimentação com data inicial {row['dt_inicial']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE bethadba.locais_mov set dt_final = '{newDtfinal}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_inicial = {row['dt_inicial']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_fim_lotacao_maior_dt_fim_contrato: {e}")
            return

        if dt_fim_lotacao_maior_dt_fim_contrato:
            dado = analisa_dt_fim_lotacao_maior_dt_fim_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_dt_fim_lotacao_maior_dt_fim_contrato(listDados=dado)

    def analisa_corrige_funcionario_sem_historico(pre_validacao):
        nomeValidacao = "Funcionário sem histórico"

        def analisa_funcionario_sem_historico():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_sem_historico(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select i_entidades as entidade,
                                            i_funcionarios as funcionario,
                                            dt_admissao as dtAlteracao,
                                            (select FIRST i_config_organ from bethadba.organogramas o where o.nivel = 2 order by o.i_organogramas asc) as configOrg,
                                            (select FIRST i_organogramas from bethadba.organogramas o where o.nivel = 2 order by o.i_organogramas asc) as organograma,
                                            (select FIRST g.i_grupos from bethadba.grupos g where g.i_entidades = entidade order by i_grupos asc) as grupo,
                                            (select FIRST i_vinculos from bethadba.vinculos v order by v.i_vinculos asc) as vinculo,
                                            optanteFgts = IF dt_opcao_fgts is null THEN 
                                                                'N'
                                                            ELSE 
                                                                'S'
                                                            ENDIF,
                                            prevFederal = 'S',
                                            prevEstadual = 'N',
                                            fundoAssis = 'N',
                                            fundoPrev = 'N',
                                            ocorrenciaSefip = 0,
                                            formaPagamento = 'D',
                                            multiTempoServ = 1,
                                            fundoFinan = 'N',
                                            categoria
                                            from bethadba.funcionarios
                                            where i_funcionarios not in (select i_funcionarios from bethadba.hist_funcionarios);
                                        """)

                for row in dados:
                    entidade = row['entidade']
                    funcionario = row['funcionario']
                    dtAlteracao = datetime_obj = datetime.datetime.combine(row['dtAlteracao'], datetime.datetime.now().time())
                    configOrg = row['configOrg']
                    organograma = row['organograma']
                    grupo = row['grupo']
                    vinculo = row['vinculo']
                    optanteFgts = row['optanteFgts']
                    prevFederal = row['prevFederal']
                    prevEstadual = row['prevEstadual']
                    fundoAssis = row['fundoAssis']
                    fundoPrev = row['fundoPrev']
                    ocorrenciaSefip = row['ocorrenciaSefip']
                    formaPagamento = row['formaPagamento']
                    multiTempoServ = row['multiTempoServ']
                    fundoFinan = row['fundoFinan']
                    categoria = row['categoria']

                    dadoAlterado.append(f"Inserido um histórico com data de {dtAlteracao} para o funcionário {entidade} da entidade {funcionario}")
                    comandoInsert += f"""insert into bethadba.hist_funcionarios (i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, 
                                            forma_pagto, multiplic, fundo_financ, categoria)
                                             values({entidade}, {funcionario}, '{dtAlteracao}', {configOrg}, {organograma}, '{grupo}', '{vinculo}', '{optanteFgts}', '{prevFederal}', '{prevEstadual}', '{fundoAssis}',
                                                    '{fundoPrev}', {ocorrenciaSefip}, '{formaPagamento}', {multiTempoServ}, '{fundoFinan}', '{categoria}');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_sem_historico: {e}")
            return

        if funcionario_sem_historico:
            dado = analisa_funcionario_sem_historico()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_sem_historico(listDados=dado)

    def analisa_corrige_funcionario_sem_historico_cargo(pre_validacao):
        nomeValidacao = "Funcionário sem histórico de cargo"

        def analisa_funcionario_sem_historico_cargo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_sem_historico_cargo(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select 
                                            i_entidades as entidade,
                                            i_funcionarios as funcionario,
                                            dt_admissao as dtAlteracao,
                                            cargo = (SELECT FIRST i_cargos from bethadba.cargos c where c.i_entidades = 1 order by i_cargos),
                                            parecerInterno = 'N',
                                            rotinaProrrogacao = 'N',
                                            rotinaRodada = 'N'
                                            from bethadba.funcionarios
                                            where i_funcionarios not in (select i_funcionarios from bethadba.hist_cargos)
                                        """)

                for row in dados:
                    entidade = row['entidade']
                    funcionario = row['funcionario']
                    dtAlteracao = datetime_obj = datetime.datetime.combine(row['dtAlteracao'], datetime.datetime.now().time())
                    cargo = row['cargo']
                    parecerInterno = row['parecerInterno'],
                    rotinaProrrogacao = row['rotinaProrrogacao'],
                    rotinaRodada = row['rotinaRodada']

                    dadoAlterado.append(f"Inserido um histórico de cargo com data de {dtAlteracao} para o funcionário {entidade} da entidade {funcionario}")
                    comandoInsert += f"""insert into bethadba.hist_cargos (i_entidades, i_funcionarios, dt_alteracoes, i_cargos, parecer_contr_interno, desconsidera_rotina_prorrogacao, desconsidera_rotina_rodada)
                                        values({entidade}, {funcionario}, '{dtAlteracao}', {cargo}, '{parecerInterno}', '{rotinaProrrogacao}', '{rotinaRodada}');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_sem_historico_cargo: {e}")
            return

        if funcionario_sem_historico_cargo:
            dado = analisa_funcionario_sem_historico_cargo()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_sem_historico_cargo(listDados=dado)

    def analisa_corrige_funcionario_sem_historico_salarial(pre_validacao):
        nomeValidacao = "Funcionário sem histórico salarial"

        def analisa_funcionario_sem_historico_salarial():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_sem_historico_salarial(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select 
                                            i_entidades as entidade,
                                            i_funcionarios as funcionario, 
                                            dt_admissao as dtAlteracao,
                                            salario = 1000.00,
                                            horaMes = 220.00,
                                            horaSem = 44.00,
                                            dt_admissao as dtChaveEsocial
                                            from bethadba.funcionarios  
                                            where i_funcionarios not in (select i_funcionarios from bethadba.hist_salariais)
                                        """)

                for row in dados:
                    entidade = row['entidade']
                    funcionario = row['funcionario']
                    dtAlteracao = datetime.datetime.combine(row['dtAlteracao'], datetime.datetime.now().time())
                    salario = row['salario'],
                    horaMes = row['horaMes'],
                    horaSem = row['horaSem'],
                    dtChaveEsocial = datetime.datetime.combine(row['dtChaveEsocial'], datetime.datetime.now().time())

                    dadoAlterado.append(f"Inserido um histórico salarial com data de {dtAlteracao} para o funcionário {entidade} da entidade {funcionario}")
                    comandoInsert += f"""INSERT into bethadba.hist_salariais (i_entidades, i_funcionarios, dt_alteracoes, salario, horas_mes, horas_sem, dt_chave_esocial)
                                        values({entidade}, {funcionario}, '{dtAlteracao}', {salario}, {horaMes}, {horaSem}, {dtChaveEsocial});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_sem_historico_salarial: {e}")
            return

        if funcionario_sem_historico_salarial:
            dado = analisa_funcionario_sem_historico_salarial()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_sem_historico_salarial(listDados=dado)

    if dadosList:
        analisa_corrige_alteracao_salario_funcionario_maior_data_rescisao(pre_validacao="alteracao_salario_funcionario_maior_data_rescisao")
        analisa_corrige_alteracao_cargo_funcionario_maior_data_rescisao(pre_validacao="alteracao_cargo_funcionario_maior_data_rescisao")
        analisa_corrige_funcionario_sem_previdencia(pre_validacao="funcionario_sem_previdencia")
        analisa_corrige_data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica(pre_validacao="data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica")
        analisa_corrige_funcionario_data_nomeacao_maior_data_posse(pre_validacao="funcionario_data_nomeacao_maior_data_posse")
        analisa_corrige_ferias_dt_gozo_fin_maior_rescisao(pre_validacao="ferias_dt_gozo_fin_maior_rescisao")
        analisa_corrige_ferias_gozo_final_apos_rescisao(pre_validacao="ferias_gozo_final_apos_rescisao")
        analisa_corrige_multiplos_locais_trabalho_principal(pre_validacao='multiplos_locais_trabalho_principal')
        analisa_corrige_funcionario_sem_local_trabalho_principal(pre_validacao='funcionario_sem_local_trabalho_principal')
        analisa_corrige_dt_fim_lotacao_maior_dt_fim_contrato(pre_validacao="dt_fim_lotacao_maior_dt_fim_contrato")
        analisa_corrige_funcionario_sem_historico(pre_validacao="funcionario_sem_historico")
        analisa_corrige_funcionario_sem_historico_cargo(pre_validacao="funcionario_sem_historico_cargo")
        analisa_corrige_funcionario_sem_historico_salarial(pre_validacao="funcionario_sem_historico_salarial")
