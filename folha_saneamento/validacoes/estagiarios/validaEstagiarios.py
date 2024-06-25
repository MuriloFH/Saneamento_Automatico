import colorama
import datetime
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import insertResponsavel, insertAgenteIntegracao


def estagiarios(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                estagiario_sem_numero_apolice_informado=False,
                estagiario_agente_integracao_nulo=False,
                estagiario_sem_responsavel=False,
                estagiario_nao_presente_na_tabela_estagios=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_estagiario_sem_numero_apolice_informado(pre_validacao):
        nomeValidacao = "Estagiário sem número da apólice de seguro informado."

        def analisa_estagiario_sem_numero_apolice_informado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_estagiario_sem_numero_apolice_informado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_entidades,
                        i_funcionarios
                    from bethadba.estagios
                    where num_apolice is null 
                    and seguro_vida = 'S'   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado seguro de vida para N para estagiario {row['i_funcionarios']} entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.estagios set seguro_vida = 'N' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_estagiario_sem_numero_apolice_informado: {e}")
            return

        if estagiario_sem_numero_apolice_informado:
            dado = analisa_estagiario_sem_numero_apolice_informado()

            if corrigirErros and len(dado) > 0:
                corrige_estagiario_sem_numero_apolice_informado(listDados=dado)

    def analisa_corrige_estagiario_agente_integracao_nulo(pre_validacao):
        nomeValidacao = "Estagiário sem agente de integração informado."

        def analisa_estagiario_agente_integracao_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_estagiario_agente_integracao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        e.i_entidades, 
                        e.i_funcionarios,
                        hf.dt_alteracoes, 
                        hf.i_agente_integracao_estagio
                    from bethadba.estagios e 
                    join bethadba.hist_funcionarios hf on(e.i_entidades = hf.i_entidades and e.i_funcionarios = hf.i_funcionarios)
                    where 
                        hf.i_agente_integracao_estagio is null
                    order by 
                        e.i_entidades, 
                        e.i_funcionarios, 
                        hf.dt_alteracoes  
                 """)

                if len(busca) > 0:
                    agenteIntegracao = banco.consultar(f"""select FIRST  i_agente_integracao_estagio 
                                                            from bethadba.hist_funcionarios 
                                                            where i_agente_integracao_estagio is not null 
                                                            group by i_agente_integracao_estagio 
                                                            order by COUNT(*) desc
                                                       """)
                    if len(agenteIntegracao) > 0:
                        agenteIntegracao = agenteIntegracao[0]['i_agente_integracao_estagio']
                    else:
                        print(colorama.Fore.RED, "Não localizado nenhum agente de integração, favor analisar os casos manualmente.", colorama.Fore.RESET)
                        return None

                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado agente de integração no histórico estagiario {row['i_funcionarios']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']} para {agenteIntegracao}")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set i_agente_integracao_estagio = {agenteIntegracao} where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_estagiario_agente_integracao_nulo: {e}")
            return

        if estagiario_agente_integracao_nulo:
            dado = analisa_estagiario_agente_integracao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_estagiario_agente_integracao_nulo(listDados=dado)

    def analisa_corrige_estagiario_sem_responsavel(pre_validacao):
        nomeValidacao = "Estagiário sem responsável informado."

        def analisa_estagiario_sem_responsavel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_estagiario_sem_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        e.i_entidades, 
                        e.i_funcionarios,
                        hf.dt_alteracoes
                    from bethadba.estagios e
                    join bethadba.hist_funcionarios hf on hf.i_entidades = e.i_entidades and hf.i_funcionarios = e.i_funcionarios
                    where 
                        hf.i_supervisor_estagio is null
                        and hf.dt_alteracoes = (select max(dt_alteracoes) from bethadba.hist_funcionarios hf
                                                               where hf.i_entidades = e.i_entidades
                                                               and hf.i_funcionarios = e.i_funcionarios)
                    order by 
                        e.i_entidades,
                        e.i_funcionarios  
                 """)

                if len(busca) > 0:
                    responsavel = banco.consultar(f"""select TOP 1 i_supervisor_estagio 
                                                            from bethadba.hist_funcionarios
                                                            where i_supervisor_estagio is not null 
                                                            GROUP BY i_supervisor_estagio 
                                                            ORDER BY COUNT(*) DESC
                                                       """)
                    if len(responsavel) > 0:
                        responsavel = responsavel[0]['i_supervisor_estagio']
                    else:
                        print(colorama.Fore.RED, "Não localizado nenhum responsável, favor analisar os casos manualmente.", colorama.Fore.RESET)
                        return None

                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado responsável no histórico estagiario {row['i_funcionarios']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']} para {responsavel}")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set i_supervisor_estagio = {responsavel} where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_estagiario_sem_responsavel: {e}")
            return

        if estagiario_sem_responsavel:
            dado = analisa_estagiario_sem_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_estagiario_sem_responsavel(listDados=dado)

    def analisa_corrige_estagiario_nao_presente_na_tabela_estagios(pre_validacao):
        nomeValidacao = "Estagiário não presente na tabela de estagios"

        def analisa_estagiario_nao_presente_na_tabela_estagios():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_estagiario_nao_presente_na_tabela_estagios(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select hist.i_entidades,
                                            hist.i_funcionarios,
                                            (SELECT i_pessoas from bethadba.funcionarios f where f.i_entidades = hist.i_entidades and f.i_funcionarios = hist.i_funcionarios) as pessoa,
                                            (SELECT dt_admissao from bethadba.funcionarios f where f.i_entidades = hist.i_entidades and f.i_funcionarios = hist.i_funcionarios) as dtAdmissao,
                                            isnull((SELECT dt_rescisao from bethadba.rescisoes r where r.i_entidades = hist.i_entidades and r.i_funcionarios = hist.i_funcionarios), null) as dtRescisao
                                            from  bethadba.hist_funcionarios hist , bethadba.vinculos v
                                            where v.i_vinculos = hist.i_vinculos
                                            and v.categoria_esocial = 901
                                            and i_funcionarios not in (select i_funcionarios from bethadba.estagios)
                                            group by hist.i_entidades, hist.i_funcionarios
                                            order by hist.i_entidades, hist.i_funcionarios
                                        """)
                formacao = banco.consultar(f"""SELECT FIRST f.i_formacoes from bethadba.formacoes f""")[0]
                instEnsino = banco.consultar(f"""SELECT top 1 pj.i_pessoas, p.nome 
                                                    FROM bethadba.pessoas_juridicas pj
                                                    join bethadba.pessoas p on p.i_pessoas = pj.i_pessoas
                                                    WHERE p.nome LIKE '%universidade federal%'
                                                    OR p.nome LIKE '%faculdade%'
                                                    OR p.nome LIKE '%ensino%'
                                                    OR p.nome LIKE '%escola%'
                                                    order by nome DESC;
                                                """)
                if len(instEnsino) == 0:
                    instEnsino = banco.consultar(f"""SELECT max(pj.i_pessoas) as iPessoa
                                                        from bethadba.pessoas_juridicas pj 
                                                """)[0]
                else:
                    instEnsino = instEnsino[0]['i_pessoas']

                for row in dados:
                    if row['dtRescisao'] is not None:
                        dtFinal = row['dtRescisao']
                    else:
                        dtFinal = row['dtAdmissao']
                        dtFinal += datetime.timedelta(days=150)

                    dadoAlterado.append(f"Inserido o estagiário {row['i_funcionarios']} da entidade {row['i_entidades']} na tabela de estagiários")
                    comandoInsert += f"""insert into bethadba.estagios (i_entidades, i_funcionarios, i_formacoes, i_pessoas, dt_inicial, dt_final, nivel_curso, periodo, fase, seguro_vida, estagio_obrigatorio)
                                            values({row['i_entidades']}, {row['i_funcionarios']}, {formacao['i_formacoes']}, {instEnsino}, '{row['dtAdmissao']}', '{dtFinal}', 4, 1, 1, 'N', 'N');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                insertAgenteIntegracao(banco)
                insertResponsavel(banco)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_estagiario_nao_presente_na_tabela_estagios: {e}")
            return

        if estagiario_nao_presente_na_tabela_estagios:
            dado = analisa_estagiario_nao_presente_na_tabela_estagios()

            if corrigirErros and len(dado) > 0:
                corrige_estagiario_nao_presente_na_tabela_estagios(listDados=dado)

    if dadosList:
        analisa_corrige_estagiario_sem_numero_apolice_informado(pre_validacao="estagiario_sem_numero_apolice_informado")
        analisa_corrige_estagiario_agente_integracao_nulo(pre_validacao="estagiario_agente_integracao_nulo")
        analisa_corrige_estagiario_sem_responsavel(pre_validacao="estagiario_sem_responsavel")
        analisa_corrige_estagiario_nao_presente_na_tabela_estagios(pre_validacao='estagiario_nao_presente_na_tabela_estagios')
