from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import reordenaSequencialAfastamentos
import datetime


def afastamentos(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 afastamentos_duplicados=False,
                 tipo_afastamento_com_classificacao_invalida=False,
                 afastamento_motivo_maximo_150_caracteres=False,
                 afastamentos_dt_afastamento_menor_dt_admissao=False,
                 tipos_afast_sem_tipo_movto_pessoal=False,
                 falta_tipo_1_com_competencia=False,
                 dt_afastamento_concomitante=False,
                 dt_afastamento_concomitante_dt_ferias=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_afastamentos_duplicados(pre_validacao):
        nomeValidacao = "Tipos de afastamentos repetidos"

        def analisa_afastamentos_duplicados():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_afastamentos_duplicados(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_tipos_afast) as tiposafs, 
                        TRIM(descricao) as descricao,
                        count(descricao) AS quantidade 
                    FROM 
                        bethadba.tipos_afast 
                    GROUP BY 
                       descricao
                    HAVING
                        quantidade > 1   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_afast = row['tiposafs'].split(',')
                        for afast in list_afast[1:]:
                            nova_descricao = row['descricao'] + ' ' + afast
                            # Tratativa leva em consideração o tamanho maximo do campo no Sybase que é 50.
                            if len(nova_descricao) <= 50:
                                dadoAlterado.append(f"Alterada descrição do tipo de afastamento {afast} de  {row['descricao']} para {nova_descricao}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_afast set descricao = '{nova_descricao}' where i_tipos_afast = {afast};\n"""
                            else:
                                sub = row['descricao'][:50 - len(afast)] + afast
                                dadoAlterado.append(f"Alterada descrição do tipo de afastamento {afast} de  {row['descricao']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_afast set descricao = '{sub}' where i_tipos_afast = {afast};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_afastamentos_duplicados: {e}")
            return

        if afastamentos_duplicados:
            dado = analisa_afastamentos_duplicados()

            if corrigirErros and len(dado) > 0:
                corrige_afastamentos_duplicados(listDados=dado)

    def analisa_corrige_tipo_afastamento_com_classificacao_invalida(pre_validacao):
        nomeValidacao = "Tipo de afastamento com classificações invalidas sendo nulo ou 1"

        def analisa_tipo_afastamento_com_classificacao_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_afastamento_com_classificacao_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        i_tipos_afast,
                        descricao,
                        classif 
                    FROM 
                        bethadba.tipos_afast
                    WHERE 
                        classif IN (1, NULL)   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada classificacao do tipo de afastamento {row['i_tipos_afast']}-{row['descricao']} para 2")
                        comandoUpdate += f"""UPDATE bethadba.tipos_afast set classif = 2 where i_tipos_afast = {row['i_tipos_afast']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_afastamento_com_classificacao_invalida: {e}")
            return

        if tipo_afastamento_com_classificacao_invalida:
            dado = analisa_tipo_afastamento_com_classificacao_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_afastamento_com_classificacao_invalida(listDados=dado)

    def analisa_corrige_afastamento_motivo_maximo_150_caracteres(pre_validacao):
        nomeValidacao = "Afastamentos com motivo superior a 150 caracteres"

        def analisa_afastamento_motivo_maximo_150_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_afastamento_motivo_maximo_150_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        LENGTH(observacao) AS tamanho_observacao, 
                        observacao,
                        i_entidades, 
                        i_funcionarios, 
                        dt_afastamento 
                    FROM
                        bethadba.afastamentos 
                    WHERE 
                        LENGTH(observacao) > 150  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado motivo do afastamento do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} data {row['dt_afastamento']} para {row['observacao'][:150]}")
                        comandoUpdate += f"""UPDATE bethadba.afastamentos set observacao = '{row['observacao'][:150]}' where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_afastamento = '{row['dt_afastamento']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_afastamento_motivo_maximo_150_caracteres: {e}")
            return

        if afastamento_motivo_maximo_150_caracteres:
            dado = analisa_afastamento_motivo_maximo_150_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_afastamento_motivo_maximo_150_caracteres(listDados=dado)

    def analisa_corrige_afastamentos_dt_afastamento_menor_dt_admissao(pre_validacao):
        nomeValidacao = "Afastamento com data inicial menor que data de admissão."

        def analisa_afastamentos_dt_afastamento_menor_dt_admissao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_afastamentos_dt_afastamento_menor_dt_admissao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        dt_afastamento, 
                        dt_ultimo_dia, 
                        i_entidades, 
                        i_tipos_afast,
                        i_funcionarios, 
                        (SELECT dt_admissao FROM bethadba.funcionarios WHERE i_funcionarios = a.i_funcionarios AND i_entidades = a.i_entidades) AS data_admissao 
                    FROM 
                        bethadba.afastamentos a
                    WHERE 
                        a.dt_afastamento < data_admissao  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de afastamento do funcionario {row['i_funcionarios']} entidade {row['i_entidades']} tipo de afastamento {row['i_tipos_afast']} de {row['dt_afastamento']} para {row['data_admissao']}")
                        comandoUpdate += f"""UPDATE bethadba.afastamentos set dt_afastamento = '{row['data_admissao']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_afastamento = '{row['dt_afastamento']}' and i_tipos_afast = {row['i_tipos_afast']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_afastamentos_dt_afastamento_menor_dt_admissao: {e}")
            return

        if afastamentos_dt_afastamento_menor_dt_admissao:
            dado = analisa_afastamentos_dt_afastamento_menor_dt_admissao()

            if corrigirErros and len(dado) > 0:
                corrige_afastamentos_dt_afastamento_menor_dt_admissao(listDados=dado)

    def analisa_corrige_tipos_afast_sem_tipo_movto_pessoal(pre_validacao):
        nomeValidacao = "Tipo do afastamento sem movimentação de pessoal informada quando o afastamento possuir um ato."

        def analisa_tipos_afast_sem_tipo_movto_pessoal():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipos_afast_sem_tipo_movto_pessoal(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select DISTINCT 
                        ta.i_tipos_afast,
                        (SELECT TOP 1 i_tipos_movpes FROM bethadba.tipos_afast where i_tipos_movpes is not null GROUP BY i_tipos_movpes ORDER BY COUNT(*) DESC) as novo_tipo_movpes
                    from bethadba.afastamentos a
                    join bethadba.tipos_afast ta on a.i_tipos_afast = ta.i_tipos_afast 
                    where 
                        a.i_atos is not null
                        and ta.i_tipos_movpes is null
                        and ta.classif not in (8,9) 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado tipo de movimentação no tipo de afastamento  {row['i_tipos_afast']} para {row['novo_tipo_movpes']}")
                        comandoUpdate += f"""UPDATE bethadba.tipos_afast set i_tipos_movpes = {row['novo_tipo_movpes']} where i_tipos_afast = {row['i_tipos_afast']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipos_afast_sem_tipo_movto_pessoal: {e}")
            return

        if tipos_afast_sem_tipo_movto_pessoal:
            dado = analisa_tipos_afast_sem_tipo_movto_pessoal()

            if corrigirErros and len(dado) > 0:
                corrige_tipos_afast_sem_tipo_movto_pessoal(listDados=dado)

    def analisa_corrige_falta_tipo_1_com_competencia(pre_validacao):
        nomeValidacao = "Faltas que tenham competência de desconto quando o tipo da falta for (1 - Em dias)."

        def analisa_falta_tipo_1_com_competencia():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_falta_tipo_1_com_competencia(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select DISTINCT 
                        ta.i_tipos_afast,
                        (SELECT TOP 1 i_tipos_movpes FROM bethadba.tipos_afast where i_tipos_movpes is not null GROUP BY i_tipos_movpes ORDER BY COUNT(*) DESC) as novo_tipo_movpes
                    from bethadba.afastamentos a
                    join bethadba.tipos_afast ta on a.i_tipos_afast = ta.i_tipos_afast 
                    where 
                        a.i_atos is not null
                        and ta.i_tipos_movpes is null
                        and ta.classif not in (8,9) 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado tipo de movimentação no tipo de afastamento  {row['i_tipos_afast']} para {row['novo_tipo_movpes']}")
                        comandoUpdate += f"""UPDATE bethadba.tipos_afast set i_tipos_movpes = {row['novo_tipo_movpes']} where i_tipos_afast = {row['i_tipos_afast']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_falta_tipo_1_com_competencia: {e}")
            return

        if falta_tipo_1_com_competencia:
            dado = analisa_falta_tipo_1_com_competencia()

            if corrigirErros and len(dado) > 0:
                corrige_falta_tipo_1_com_competencia(listDados=dado)

    def analisa_corrige_dt_afastamento_concomitante(pre_validacao):
        nomeValidacao = "Data de afastamento concomitante."

        def analisa_dt_afastamento_concomitante():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_afastamento_concomitante(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select
                                            a.i_tipos_afast,
                                            a.i_entidades,
                                            a.i_funcionarios,
                                            a.dt_afastamento as aDtAfastamnento,
                                            b.dt_afastamento as bDtAfastamento,
                                            isnull(a.dt_ultimo_dia, a.dt_afastamento) as aDt_ultimo_dia,
                                            isnull(b.dt_ultimo_dia, b.dt_afastamento) as bDt_ultimo_dia,
                                            temRescisao = (select 1 from bethadba.tipos_afast ta where ta.i_tipos_afast = a.i_tipos_afast and ta.classif = 8),
                                            trabalhouUltimoDia = if temRescisao is not null THEN
                                                                    (select r.trab_dia_resc from bethadba.rescisoes r
                                                                      join bethadba.motivos_resc mr on r.i_motivos_resc = mr.i_motivos_resc
                                                                      join bethadba.tipos_afast ta2 on mr.i_tipos_afast = ta2.i_tipos_afast
                                                                      where r.i_entidades = a.i_entidades and r.i_funcionarios = a.i_funcionarios
                                                                      and ta2.classif in (8,9))
                                                                else 'N' endif
                                                from bethadba.afastamentos a
                                            join bethadba.afastamentos b on a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios
                                            where (a.dt_afastamento BETWEEN b.dt_afastamento and b.dt_ultimo_dia or a.dt_ultimo_dia BETWEEN b.dt_afastamento and b.dt_ultimo_dia)
                                            and (a.dt_afastamento <> b.dt_afastamento or a.dt_ultimo_dia <> b.dt_ultimo_dia)
                                            and temRescisao is null
                                            and (trabalhouUltimoDia is null or trabalhouUltimoDia = 'N')
                                        """)
                listFuncionarios = []
                for row in dados:
                    print(row)
                    entidade = row['i_entidades']
                    funcionario = row['i_funcionarios']
                    dtAfastamento = row['aDtAfastamnento']
                    listFuncionarios.append(funcionario)

                    lastAfastamento = banco.consultar(f"""SELECT min(dt_afastamento) as dtAfastamento
                                                            from bethadba.afastamentos a
                                                            where a.i_entidades = {entidade} and i_funcionarios = {funcionario} and dt_afastamento <= '{dtAfastamento}'
                                                        """)
                    #
                    if len(lastAfastamento) > 0:
                        newLastDtAfastamento = lastAfastamento[0]['dtAfastamento']
                        newLastDtAfastamento -= datetime.timedelta(days=30)

                        newDtUltimoDiaAfastamento = newLastDtAfastamento
                        newDtUltimoDiaAfastamento += datetime.timedelta(days=5)

                        dadoAlterado.append(f"Alterado a data de afastamento para {newLastDtAfastamento} e a data do ultimo dia para {newDtUltimoDiaAfastamento} do funcionário {funcionario} da entidade {entidade}")
                        comandoUpdate = f"""UPDATE bethadba.afastamentos set dt_afastamento = '{newLastDtAfastamento}', dt_ultimo_dia = '{newDtUltimoDiaAfastamento}' where i_entidades = {entidade} and i_funcionarios = {funcionario} and dt_afastamento = '{dtAfastamento}';\n"""

                        banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                             tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    reordenaSequencialAfastamentos(entidade=entidade, funcionario=funcionario, banco=banco)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_afastamento_concomitante: {e}")
            return

        if dt_afastamento_concomitante:
            dado = analisa_dt_afastamento_concomitante()

            if corrigirErros and len(dado) > 0:
                corrige_dt_afastamento_concomitante(listDados=dado)

    def analisa_corrige_dt_afastamento_concomitante_dt_ferias(pre_validacao):
        nomeValidacao = "Data de afastamento concomitante com data de férias"

        def analisa_dt_afastamento_concomitante_dt_ferias():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_afastamento_concomitante_dt_ferias(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select
                                            a.i_tipos_afast,
                                            a.i_entidades,
                                            a.i_funcionarios,
                                            a.dt_afastamento as aDtAfastamnento,
                                            b.dt_afastamento as bDtAfastamento,
                                            isnull(a.dt_ultimo_dia, a.dt_afastamento) as aDt_ultimo_dia,
                                            isnull(b.dt_ultimo_dia, b.dt_afastamento) as bDt_ultimo_dia,
                                            temRescisao = (select 1 from bethadba.tipos_afast ta where ta.i_tipos_afast = a.i_tipos_afast and ta.classif = 8),
                                            trabalhouUltimoDia = if temRescisao is not null THEN
                                                                    (select r.trab_dia_resc from bethadba.rescisoes r
                                                                      join bethadba.motivos_resc mr on r.i_motivos_resc = mr.i_motivos_resc
                                                                      join bethadba.tipos_afast ta2 on mr.i_tipos_afast = ta2.i_tipos_afast
                                                                      where r.i_entidades = a.i_entidades and r.i_funcionarios = a.i_funcionarios
                                                                      and ta2.classif in (8,9))
                                                                else 'N' endif
                                                from bethadba.afastamentos a
                                            join bethadba.afastamentos b on a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios
                                            where (a.dt_afastamento BETWEEN b.dt_afastamento and b.dt_ultimo_dia or a.dt_ultimo_dia BETWEEN b.dt_afastamento and b.dt_ultimo_dia)
                                            and (a.dt_afastamento <> b.dt_afastamento or a.dt_ultimo_dia <> b.dt_ultimo_dia)
                                            and temRescisao is null
                                            and (trabalhouUltimoDia is null or trabalhouUltimoDia = 'N')
                                        """)
                listFuncionarios = []
                for row in dados:
                    print(row)
                    entidade = row['i_entidades']
                    funcionario = row['i_funcionarios']
                    dtAfastamento = row['aDtAfastamnento']
                    listFuncionarios.append(funcionario)

                    lastAfastamento = banco.consultar(f"""SELECT min(dt_afastamento) as dtAfastamento
                                                            from bethadba.afastamentos a
                                                            where a.i_entidades = {entidade} and i_funcionarios = {funcionario} and dt_afastamento <= '{dtAfastamento}'
                                                        """)
                    #
                    if len(lastAfastamento) > 0:
                        newLastDtAfastamento = lastAfastamento[0]['dtAfastamento']
                        newLastDtAfastamento -= datetime.timedelta(days=30)

                        newDtUltimoDiaAfastamento = newLastDtAfastamento
                        newDtUltimoDiaAfastamento += datetime.timedelta(days=5)

                        dadoAlterado.append(f"Alterado a data de afastamento para {newLastDtAfastamento} e a data do ultimo dia para {newDtUltimoDiaAfastamento} do funcionário {funcionario} da entidade {entidade}")
                        comandoUpdate = f"""UPDATE bethadba.afastamentos set dt_afastamento = '{newLastDtAfastamento}', dt_ultimo_dia = '{newDtUltimoDiaAfastamento}' where i_entidades = {entidade} and i_funcionarios = {funcionario} and dt_afastamento = '{dtAfastamento}';\n"""

                        banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                             tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    reordenaSequencialAfastamentos(entidade=entidade, funcionario=funcionario, banco=banco)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_afastamento_concomitante_dt_ferias: {e}")
            return

        if dt_afastamento_concomitante_dt_ferias:
            dado = analisa_dt_afastamento_concomitante_dt_ferias()

            if corrigirErros and len(dado) > 0:
                corrige_dt_afastamento_concomitante_dt_ferias(listDados=dado)

    if dadosList:
        analisa_corrige_afastamentos_duplicados(pre_validacao="afastamentos_duplicados")
        analisa_corrige_tipo_afastamento_com_classificacao_invalida(pre_validacao="tipo_afastamento_com_classificacao_invalida")
        analisa_corrige_afastamento_motivo_maximo_150_caracteres(pre_validacao="afastamento_motivo_maximo_150_caracteres")
        analisa_corrige_afastamentos_dt_afastamento_menor_dt_admissao(pre_validacao="afastamentos_dt_afastamento_menor_dt_admissao")
        analisa_corrige_tipos_afast_sem_tipo_movto_pessoal(pre_validacao="tipos_afast_sem_tipo_movto_pessoal")
        analisa_corrige_falta_tipo_1_com_competencia(pre_validacao="falta_tipo_1_com_competencia")
        analisa_corrige_dt_afastamento_concomitante(pre_validacao="dt_afastamento_concomitante")
        analisa_corrige_dt_afastamento_concomitante_dt_ferias(pre_validacao="dt_afastamento_concomitante_dt_ferias")
