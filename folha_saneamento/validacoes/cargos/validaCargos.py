from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def cargos(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
           corrigirErros=False,
           cbo_nulo=False,
           cargo_descricao_duplicada=False,
           cargo_sem_configuracao_de_ferias=False,
           quantidade_vaga_maior_9999=False
           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cbo_nulo(pre_validacao):
        nomeValidacao = "CBO's nulos nos cargos."

        def analisa_cbo_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cbo_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades,
                        i_cargos,
                        i_cbo,
                        i_tipos_cargos,
                        nome,
                        (SELECT TOP 1 i_cbo FROM bethadba.cargos where i_cbo is not null GROUP BY i_cbo ORDER BY COUNT(i_cbo) DESC) as novo_cbo
                    FROM 
                        bethadba.cargos 
                    WHERE 
                        i_cbo IS NULL   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado CBO do cargo {row['i_cargos']} - {row['nome']} de nulo para {row['novo_cbo']} entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.cargos set i_cbo = {row['novo_cbo']} where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cbo_nulo: {e}")
            return

        if cbo_nulo:
            dado = analisa_cbo_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cbo_nulo(listDados=dado)

    def analisa_corrige_cargo_descricao_duplicada(pre_validacao):
        nomeValidacao = "Cargos com descrição duplicada."

        def analisa_cargo_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cargo_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        c.i_entidades,
                        c.nome,
                        count(c.nome) as quantidade,
                        list(c.i_cargos) as codigos
                    from bethadba.cargos c
                    group by c.i_entidades, c.nome
                    having count(c.nome) > 1  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_cargos = row['codigos'].split(',')
                        for cargo in list_cargos[1:]:
                            dadoAlterado.append(f"Alterada descrição do cargo {cargo}-{row['nome']} para {row['nome'] + ' ' + cargo} na entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE bethadba.cargos set nome = '{row['nome'] + ' ' + cargo}' where i_cargos = {cargo} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cargo_descricao_duplicada: {e}")
            return

        if cargo_descricao_duplicada:
            dado = analisa_cargo_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_cargo_descricao_duplicada(listDados=dado)

    def analisa_corrige_cargo_sem_configuracao_de_ferias(pre_validacao):
        nomeValidacao = "Cargos sem configuração de férias."

        def analisa_cargo_sem_configuracao_de_ferias():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cargo_sem_configuracao_de_ferias(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        hc.i_cargos, 
                        hc.i_entidades , 
                        hc.i_config_ferias , 
                        hc.i_config_ferias_subst,
                        (SELECT TOP 1 i_config_ferias FROM bethadba.cargos_compl where i_config_ferias is not null GROUP BY i_config_ferias ORDER BY COUNT(*) DESC) as nova_config_ferias,
                        (SELECT TOP 1 i_config_ferias_subst FROM bethadba.cargos_compl where i_config_ferias_subst is not null GROUP BY i_config_ferias_subst ORDER BY COUNT(*) DESC) as nova_config_ferias_subst
                    FROM 
                        bethadba.cargos_compl hc
                        left join bethadba.cargos c on (c.i_cargos = hc.i_cargos) 
                    WHERE
                        (hc.i_config_ferias IS NULL 
                        OR hc.i_config_ferias_subst IS NULL) 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        if row['i_config_ferias'] is None and row['i_config_ferias_subst'] is None:
                            dadoAlterado.append(f"Alterada configuração de férias para {row['nova_config_ferias']} e configuração de ferias(substitutos) para {row['nova_config_ferias_subst']} do cargo {row['i_cargos']} entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE bethadba.cargos_compl set i_config_ferias = {row['nova_config_ferias']}, i_config_ferias_subst = {row['nova_config_ferias_subst']} where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']};\n"""
                        elif row['i_config_ferias'] is None and row['i_config_ferias_subst'] is not None:
                            dadoAlterado.append(
                                f"Alterada configuração de férias para {row['nova_config_ferias']} do cargo {row['i_cargos']} entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE bethadba.cargos_compl set i_config_ferias = {row['nova_config_ferias']} where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']};\n"""
                        else:
                            dadoAlterado.append(
                                f"Alterada configuração de ferias(substitutos) para {row['nova_config_ferias_subst']} do cargo {row['i_cargos']} entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE bethadba.cargos_compl set i_config_ferias_subst = {row['nova_config_ferias_subst']} where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cargo_sem_configuracao_de_ferias: {e}")
            return

        if cargo_sem_configuracao_de_ferias:
            dado = analisa_cargo_sem_configuracao_de_ferias()

            if corrigirErros and len(dado) > 0:
                corrige_cargo_sem_configuracao_de_ferias(listDados=dado)

    def analisa_corrige_quantidade_vaga_maior_9999(pre_validacao):
        nomeValidacao = "Cargo com quantidade de vaga maior do que 9999."

        def analisa_quantidade_vaga_maior_9999():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_quantidade_vaga_maior_9999(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        c1.i_cargos,
                        c1.i_entidades,
                        null as dt_alteracoes,
                        isnull(c1.qtd_vagas,0) as qtd_vagas,
                        'qtd_vagas' as coluna,
                        'cargos_compl' as tabela
                    FROM bethadba.cargos_compl c1
                    where c1.qtd_vagas> 9999
                    
                    union
                    
                    SELECT 
                        c1.i_cargos,
                        c1.i_entidades,
                        c1.dt_alteracoes,
                        isnull(c1.vagas_acresc,0) as qtd_vagas,
                        'vagas_acresc' as coluna,
                        'mov_cargos' as tabela
                    FROM bethadba.mov_cargos c1
                    where c1.vagas_acresc> 9999 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        if row['tabela'] == 'cargos_compl':
                            dadoAlterado.append(f"Alterada quantidade de vagas do cargo {row['i_cargos']} entidade {row['i_entidades']} para 9999")
                            comandoUpdate += f"""UPDATE bethadba.cargos_compl set qtd_vagas = 9999 where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']};\n"""
                        else:
                            dadoAlterado.append(f"Alterada quantidade de vagas acrescidas do cargo {row['i_cargos']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']} para 9999")
                            comandoUpdate += f"""UPDATE bethadba.mov_cargos set vagas_acresc = 9999 where i_cargos = {row['i_cargos']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_quantidade_vaga_maior_9999: {e}")
            return

        if quantidade_vaga_maior_9999:
            dado = analisa_quantidade_vaga_maior_9999()

            if corrigirErros and len(dado) > 0:
                corrige_quantidade_vaga_maior_9999(listDados=dado)

    if dadosList:
        analisa_corrige_cbo_nulo(pre_validacao="cbo_nulo")
        analisa_corrige_cargo_descricao_duplicada(pre_validacao="cargo_descricao_duplicada")
        analisa_corrige_cargo_sem_configuracao_de_ferias(pre_validacao="cargo_sem_configuracao_de_ferias")
        analisa_corrige_quantidade_vaga_maior_9999(pre_validacao="quantidade_vaga_maior_9999")