from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import removeDiasData
import colorama


def pensionista(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                pensionista_menor_18_ano_sem_responsavel=False,
                tipo_pensao_nulo=False,
                pensionista_nao_cadastrado_na_tabela=False,
                pensionista_sem_cessacao=False,
                pensionista_sem_dependente=False,
                instituidor_sem_rescisao_por_aposentadoria=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_pensionista_menor_18_ano_sem_responsavel(pre_validacao):
        nomeValidacao = "Responsável não informado para beneficiário menor de 18 anos"

        def analisa_pensionista_menor_18_ano_sem_responsavel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pensionista_menor_18_ano_sem_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select
                                                beneficiarios.i_entidades as entidade,
                                                beneficiarios.i_funcionarios as beneficiario,
                                                pessoas_fisicas.i_pessoas as PessoasFisicas,
                                                pessoas_fisicas.dt_nascimento as DataNascimento,
                                                datename(year, getdate()) - datename(year, dt_nascimento) as IdadePensionista,
                                                beneficiarios.i_instituidor as instituidor,
                                                (SELECT i_pessoas from bethadba.funcionarios f where f.i_entidades = entidade and f.i_funcionarios = instituidor) as pessoaInstituidor,
                                                (select i_pessoas from bethadba.beneficiarios_repres_legal brl where beneficiarios.i_entidades = brl.i_entidades and beneficiarios.i_funcionarios = brl.i_funcionarios) as responsavel

                                            from
                                                bethadba.beneficiarios,
                                                bethadba.funcionarios,
                                                bethadba.pessoas_fisicas
                                            where
                                                funcionarios.i_funcionarios = beneficiarios.i_funcionarios
                                                and pessoas_fisicas.i_pessoas = funcionarios.i_pessoas
                                                and idadePensionista < 18 and responsavel is NULL 
                                        """)

                for row in dados:
                    dadoAlterado.append(f"""Inserido o representante legal {row['pessoaInstituidor']} para o beneficiário {row['beneficiario']} da entidade {row['entidade']}""")
                    comandoInsert += f"""insert into bethadba.beneficiarios_repres_legal (i_entidades, i_funcionarios, i_pessoas, tipo, dt_inicial)
                                            values ({row['entidade']}, {row['beneficiario']}, {row['pessoaInstituidor']}, 2, '{row['DataNascimento']}');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pensionista_menor_18_ano_sem_responsavel: {e}")
            return

        if pensionista_menor_18_ano_sem_responsavel:
            dado = analisa_pensionista_menor_18_ano_sem_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_pensionista_menor_18_ano_sem_responsavel(listDados=dado)

    def analisa_corrige_tipo_pensao_nulo(pre_validacao):
        nomeValidacao = "Tipo pensão vazio para pensionistas."

        def analisa_tipo_pensao_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_pensao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select f.i_entidades,
                                            f.i_funcionarios,
                                            v.tipo_func,
                                            f.tipo_pens,
                                            v.categoria_esocial
                                            from bethadba.funcionarios f
                                            join bethadba.hist_funcionarios hf on f.i_funcionarios = hf.i_funcionarios and f.i_entidades = hf.i_entidades 
                                            join bethadba.vinculos v on hf.i_vinculos = v.i_vinculos 
                                            where v.tipo_func = 'B'
                                            and f.tipo_pens is null
                                            and v.categoria_esocial is null
                                        """)

                for row in dados:
                    dadoAlterado.append(f"Alterado o tipo de pensão para 1 do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE bethadba.funcionarios set tipo_pens = 1 where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_pensao_nulo: {e}")
            return

        if tipo_pensao_nulo:
            dado = analisa_tipo_pensao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_pensao_nulo(listDados=dado)

    def analisa_corrige_pensionista_nao_cadastrado_na_tabela(pre_validacao):
        nomeValidacao = "Pensionista não cadastrado na tabela de beneficiários."

        def analisa_pensionista_nao_cadastrado_na_tabela():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pensionista_nao_cadastrado_na_tabela(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                                f.i_entidades,
                                                f.i_funcionarios,
                                                f.i_pessoas,
                                                f.tipo_func,
                                                f.tipo_pens
                                            from bethadba.funcionarios f 
                                            where f.tipo_pens in (1,2)
                                                and f.tipo_func = 'B'
                                                and not exists (select 1 from bethadba.beneficiarios b where f.i_entidades = b.i_entidades and f.i_funcionarios = b.i_funcionarios)
                                                order by 1,2 asc
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        print(row)
                        dependente = row['i_pessoas']

                        buscaInstituidor = banco.consultar(f"""SELECT f.i_funcionarios, f.i_entidades
                                                                from bethadba.dependentes d
                                                                join bethadba.funcionarios f on (f.i_pessoas = d.i_pessoas and f.i_entidades = 1)
                                                                where d.i_dependentes = {dependente}
                                                            """)
                        if len(buscaInstituidor) > 0:
                            buscaInstituidor = buscaInstituidor[0]

                            dadoAlterado.append(f"Inserido a matricula {row['i_funcionarios']} como beneficiário(a) na entidade {row['i_entidades']}")
                            comandoInsert += f"""insert into bethadba.beneficiarios (i_entidades, i_funcionarios, i_entidades_inst, i_instituidor, duracao_ben, perc_recebto, config, parecer_interno, situacao, matricula_instituidor)
                                                  values({row['i_entidades']}, {row['i_funcionarios']}, {buscaInstituidor['i_entidades']}, {buscaInstituidor['i_funcionarios']}, 'V', 30, 1, 'F', 3, {buscaInstituidor['i_funcionarios']});\n"""
                        else:
                            print(colorama.Fore.RED, f"Não localizado nenhum dado na tabela de dependentes para a pessoa {dependente} na entidade {row['i_entidades']}, favor analisar manualmente o caso.")

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pensionista_nao_cadastrado_na_tabela: {e}")
            return

        if pensionista_nao_cadastrado_na_tabela:
            dado = analisa_pensionista_nao_cadastrado_na_tabela()

            if corrigirErros and len(dado) > 0:
                corrige_pensionista_nao_cadastrado_na_tabela(listDados=dado)

    def analisa_corrige_pensionista_sem_cessacao(pre_validacao):
        nomeValidacao = "Pensionista sem cessação de benefício cadastrado."

        def analisa_pensionista_sem_cessacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pensionista_sem_cessacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                                  f.i_entidades, 
                                                  f.i_funcionarios,
                                                  f.dt_admissao,
                                                  b.i_entidades_inst,
                                                  b.i_instituidor,
                                                  f.tipo_pens
                                            from bethadba.funcionarios f 
                                            join bethadba.beneficiarios b on f.i_entidades = b.i_entidades and f.i_funcionarios = b.i_funcionarios 
                                            where f.tipo_func = 'B' 
                                            and f.tipo_pens = 1
                                            and exists (select 1 from bethadba.rescisoes r 
                                                    join bethadba.motivos_apos ma on r.i_motivos_apos = ma.i_motivos_apos 
                                                        join bethadba.tipos_afast ta on ma.i_tipos_afast = ta.i_tipos_afast
                                                        where r.i_entidades = b.i_entidades_inst 
                                                        and r.i_funcionarios = b.i_instituidor
                                                        and r.i_motivos_apos is not null and r.dt_canc_resc is null
                                                        and ta.classif = 9)
                                            and not exists (select 1
                                                                 from bethadba.rescisoes resc 
                                                                 join bethadba.motivos_resc mot on (resc.i_motivos_resc = mot.i_motivos_resc)
                                                            where resc.i_entidades = b.i_entidades_inst 
                                                            and resc.i_funcionarios = b.i_instituidor 
                                                            and mot.dispensados = 4                                
                                                            and resc.dt_canc_resc is null)
                                            order by 1,2 asc
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        entidadeInst = row['i_entidades_inst']
                        funcInstituidor = row['i_instituidor']

                        dtRescisao = removeDiasData(row['dt_admissao'], 1)

                        lasRescInstituidor = banco.consultar(f"""SELECT first i_rescisoes
                                                                    from bethadba.rescisoes r 
                                                                    where r.i_entidades = 1
                                                                    and r.i_funcionarios = 83
                                                                    order by r.i_rescisoes desc
                                                             """)

                        if len(lasRescInstituidor) > 0:
                            iRescisoes = (int(lasRescInstituidor[0]['i_rescisoes']) + 1)
                        else:
                            iRescisoes = 1

                        comandoInsert += f"""INSERT INTO bethadba.rescisoes(i_entidades, i_funcionarios, i_rescisoes, i_motivos_resc, dt_rescisao, aviso_ind, dt_aviso, vlr_saldo_fgts, fgts_mesant, compl_mensal, trab_dia_resc, aviso_desc)
                                            values({entidadeInst}, {funcInstituidor}, {iRescisoes}, 8, {dtRescisao}, 'N', {dtRescisao}, 0, 'S', 'N', 'N', 'N');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pensionista_sem_cessacao: {e}")
            return

        if pensionista_sem_cessacao:
            dado = analisa_pensionista_sem_cessacao()

            if corrigirErros and len(dado) > 0:
                corrige_pensionista_sem_cessacao(listDados=dado)

    def analisa_corrige_pensionista_sem_dependente(pre_validacao):
        nomeValidacao = "Pensionista sem dependente."

        def analisa_pensionista_sem_dependente():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_pensionista_sem_dependente(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                            f.i_funcionarios,
                                            b.i_instituidor,
                                            f.i_pessoas,
                                            pessoaInstituidor = (select f2.i_pessoas from bethadba.funcionarios f2
                                                                   where f2.i_entidades = b.i_entidades_inst and f2.i_funcionarios = b.i_instituidor)
                                            from bethadba.funcionarios f
                                            join bethadba.beneficiarios b on f.i_entidades = b.i_entidades  and f.i_funcionarios = b.i_funcionarios 
                                            where f.tipo_func = 'B' 
                                            and f.tipo_pens in (1,2)
                                            and not exists (select 1 from bethadba.dependentes d
                                                             where d.i_pessoas = pessoaInstituidor and d.i_dependentes = f.i_pessoas)
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        instituidor = row['pessoaInstituidor']
                        beneficiario = row['i_funcionarios']

                        comandoInsert += f"""insert into bethadba.dependentes (i_pessoas, i_dependentes, grau)
                                                values({instituidor}, {beneficiario}, 10);\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_pensionista_sem_dependente: {e}")
            return

        if pensionista_sem_dependente:
            dado = analisa_pensionista_sem_dependente()

            if corrigirErros and len(dado) > 0:
                corrige_pensionista_sem_dependente(listDados=dado)

    def analisa_corrige_instituidor_sem_rescisao_por_aposentadoria(pre_validacao):
        nomeValidacao = "Pensionista sem rescisão por aposentadoria."

        def analisa_instituidor_sem_rescisao_por_aposentadoria():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_instituidor_sem_rescisao_por_aposentadoria(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                            f.i_entidades, 
                                            f.i_funcionarios,
                                            b.i_entidades_inst,
                                            b.i_instituidor,
                                            f.tipo_pens
                                            from bethadba.funcionarios f 
                                            join bethadba.beneficiarios b on f.i_entidades = b.i_entidades and f.i_funcionarios = b.i_funcionarios 
                                            where f.tipo_func = 'B' 
                                            and f.tipo_pens in (1,2)
                                            and exists (select 1 from bethadba.rescisoes r 
                                                join bethadba.motivos_apos ma on r.i_motivos_apos = ma.i_motivos_apos 
                                                    join bethadba.tipos_afast ta on ma.i_tipos_afast = ta.i_tipos_afast
                                                            where r.i_entidades = b.i_entidades_inst and r.i_funcionarios = b.i_instituidor
                                                            and r.i_motivos_apos is not null and r.dt_canc_resc is null
                                                            and ta.classif = 9)
                                            and  not exists (select 1
                                                                 from bethadba.afastamentos a
                                                                 join bethadba.tipos_afast ta2 on a.i_tipos_afast = ta2.i_tipos_afast
                                                                 where a.i_entidades = b.i_entidades_inst and a.i_funcionarios = b.i_instituidor
                                                                 and ta2.classif = 9)
                                            order by 1,2 asc
                                        """)
                tipo_afast = banco.consultar(f"""SELECT first i_tipos_afast
                                                    from bethadba.tipos_afast ta 
                                                    where ta.classif = 9
                                            """)[0]['i_tipos_afast']

                if len(busca) > 0:
                    for row in busca:
                        entInstituidor = row['i_entidades_inst']
                        instituidor = row['i_instituidor']

                        afastInstituidor = banco.consultar(f"""SELECT a.i_entidades, a.i_funcionarios, a.i_tipos_afast, a.dt_afastamento, a.dt_ultimo_dia
                                                                from bethadba.afastamentos a 
                                                                where a.i_entidades = {row['i_entidades_inst']} and a.i_funcionarios = {row['i_instituidor']}
                                                                order by dt_afastamento desc
                                                            """)

                        if len(afastInstituidor) > 1:
                            ultimoAfast = afastInstituidor[0]
                            penultimoAfasta = afastInstituidor[1]

                            if penultimoAfasta['dt_ultimo_dia'] is not None:
                                difDtAfast = ultimoAfast['dt_afastamento'] - penultimoAfasta['dt_ultimo_dia']
                            else:
                                difDtAfast = ultimoAfast['dt_afastamento'] - penultimoAfasta['dt_afastamento']

                            difDtAfast = (int(difDtAfast.days) - 1)
                            dtAfastAposento = removeDiasData(ultimoAfast['dt_afastamento'], difDtAfast)

                            dtUltimoDiaAposento = removeDiasData(ultimoAfast['dt_afastamento'], 1)

                        elif len(afastInstituidor) == 1:
                            ultimoAfast = afastInstituidor[0]
                            dtAfastAposento = removeDiasData(ultimoAfast['dt_afastamento'], 30)
                            dtUltimoDiaAposento = removeDiasData(ultimoAfast['dt_afastamento'], 1)

                        else:
                            dtAdmissaoBeneficiario = banco.consultar(f"""SELECT first f.dt_admissao
                                                                            from bethadba.funcionarios f 
                                                                            where f.i_entidades = {row['i_entidades']} and f.i_funcionarios = {row['i_funcionarios']}
                                                                            order by dt_admissao desc
                                                                     """)
                            dtAfastAposento = removeDiasData(dtAdmissaoBeneficiario[0]['dt_admissao'], 2)
                            dtUltimoDiaAposento = dtAfastAposento

                        comandoInsert += f"""insert into bethadba.afastamentos (i_entidades, i_funcionarios, dt_afastamento, dt_ultimo_dia, i_tipos_afast)
                                            values({entInstituidor}, {instituidor}, '{dtAfastAposento}', '{dtUltimoDiaAposento}', {tipo_afast});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_instituidor_sem_rescisao_por_aposentadoria: {e}")
            return

        if instituidor_sem_rescisao_por_aposentadoria:
            dado = analisa_instituidor_sem_rescisao_por_aposentadoria()

            if corrigirErros and len(dado) > 0:
                corrige_instituidor_sem_rescisao_por_aposentadoria(listDados=dado)

    if dadosList:
        analisa_corrige_pensionista_menor_18_ano_sem_responsavel(pre_validacao="pensionista_menor_18_ano_sem_responsavel")
        analisa_corrige_tipo_pensao_nulo(pre_validacao="tipo_pensao_nulo")
        analisa_corrige_pensionista_nao_cadastrado_na_tabela(pre_validacao="pensionista_nao_cadastrado_na_tabela")
        analisa_corrige_pensionista_sem_cessacao(pre_validacao="pensionista_sem_cessacao")
        analisa_corrige_pensionista_sem_dependente(pre_validacao="pensionista_sem_dependente")
        analisa_corrige_instituidor_sem_rescisao_por_aposentadoria(pre_validacao="instituidor_sem_rescisao_por_aposentadoria")
