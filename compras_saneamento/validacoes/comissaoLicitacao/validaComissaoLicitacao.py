from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl


def comissoesLicitacao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       cpf_autoridade_invalido=False,
                       cpf_presidente_invalido=False,
                       data_expiracao_nula=False,
                       tipo_comissao_diferente_do_pregao=False,
                       comissao_sem_data_criacao_ou_designacao=False,
                       comissao_sem_responsavel_assinatura=False,
                       tipo_comissao_diferente_leiloeiro=False,
                       data_exoneracao_inferior_data_processo=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cpf_autoridade_invalido(pre_validacao):
        nomeValidacao = "O cpf da autoridade da comissão é inválido"

        def analisa_cpf_autoridade_invalido():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_autoridade_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select  
                                                i_responsavel, 
                                                cpf_titular,
                                                compras.dbf_valida_cgc_cpf(null,cpf_titular,'F','S') as valida_cpf,
                                                i_entidades
                                            from compras.responsaveis
                                            where valida_cpf = 0   
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.with_columns(
                        pl.col('i_responsavel').map_elements(lambda x: geraCfp()).alias('novo_cpf')
                    )
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado CPF da autoridade  {row['i_responsavel']} entidade {row['i_entidades']} de {row['cpf_titular']} para {row['novo_cpf']}")
                        comandoUpdate += f"""update compras.responsaveis set cpf_titular = '{row['novo_cpf']}' where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_autoridade_invalido: {e}")
            return

        if cpf_autoridade_invalido:
            dado = analisa_cpf_autoridade_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_autoridade_invalido(listDados=dado)

    def analisa_corrige_cpf_presidente_invalido(pre_validacao):
        nomeValidacao = "O cpf do presidente da comissão é inválido"

        def analisa_cpf_presidente_invalido():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_presidente_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select  
                                                i_responsavel, 
                                                cpf_presid,
                                                compras.dbf_valida_cgc_cpf(null,cpf_presid,'F','S') as valida_cpf,
                                                i_entidades
                                            from compras.responsaveis
                                            where valida_cpf = 0   
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.with_columns(
                        pl.col('i_responsavel').map_elements(lambda x: geraCfp()).alias('novo_cpf')
                    )
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado CPF da autoridade  {row['i_responsavel']} entidade {row['i_entidades']} de {row['cpf_presid']} para {row['novo_cpf']}")
                        comandoUpdate += f"""update compras.responsaveis set cpf_presid = '{row['novo_cpf']}' where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_presidente_invalido: {e}")
            return

        if cpf_presidente_invalido:
            dado = analisa_cpf_presidente_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_presidente_invalido(listDados=dado)

    def analisa_corrige_data_expiracao_nula(pre_validacao):
        nomeValidacao = "A data de expiração da comissão não pode ser nula"

        def analisa_data_expiracao_nula():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_expiracao_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select  
                                                i_responsavel, 
                                                isnull(dateformat(data_expira, 'yyyy-mm-dd'), '') as dataExpiracao,
                                                isnull(dateformat(responsaveis.data_publ,'yyyy-mm-dd'),dateformat(responsaveis.data_desig,'yyyy-mm-dd'),'') as dataCriacao,
                                                year(dataCriacao) + 4 || RIGHT(dataCriacao, 6) as nova_data_expiracao,
                                                i_entidades
                                            from compras.responsaveis
                                            where dataExpiracao = ''
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de expiração da comissão  {row['i_responsavel']} entidade {row['i_entidades']} para {row['nova_data_expiracao']}")
                        comandoUpdate += f"""update compras.responsaveis set data_expira = '{row['nova_data_expiracao']}' where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_expiracao_nula: {e}")
            return

        if data_expiracao_nula:
            dado = analisa_data_expiracao_nula()

            if corrigirErros and len(dado) > 0:
                corrige_data_expiracao_nula(listDados=dado)

    def analisa_corrige_tipo_comissao_diferente_do_pregao(pre_validacao):
        nomeValidacao = "Tipo de comissão diferente de Pregão"

        def analisa_tipo_comissao_diferente_do_pregao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_comissao_diferente_do_pregao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select distinct
                                                r.i_responsavel, 
                                                r.tipo_comissao,
                                                p.modalidade,
                                                p.i_processo,
                                                p.i_ano_proc,
                                                case 
                                                    when (r.tipo_comissao <> 'G') and (p.modalidade in (13,14)) then isnull(p.i_processo,0)
                                                    else 0
                                                end  as tipoIncompativel,
                                                r.i_entidades
                                            from compras.responsaveis r,compras.processos p
                                            where r.i_responsavel = p.i_responsavel
                                            and r.i_entidades = p.i_entidades
                                            and tipoIncompativel > 0
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df_resp = df.unique(subset=['i_responsavel', 'i_entidades'])
                    for resp in df_resp.iter_rows(named=True):
                        copy_responsavel = banco.consultar(f"select * from compras.responsaveis where i_responsavel = {resp['i_responsavel']} and i_entidades = {resp['i_entidades']};")
                        id_maximo = banco.consultar(f"select max(i_responsavel) as id from compras.responsaveis where i_entidades = {resp['i_entidades']};")
                        id = id_maximo[0]['id'] + 1
                        df_responsavel = pl.DataFrame(copy_responsavel)

                        for row in df_responsavel.iter_rows(named=True):
                            tipo_comissao = 'G'
                            # Insert
                            dadoInserido = []
                            dadoInserido.append(f"Realizado cadastro da comissão {id} com tipo_comissao = G sendo igual a comissao {resp['i_responsavel']}")
                            comandoInsert = f"INSERT INTO compras.responsaveis VALUES({id},'{row['nome_titular']}','{row['cargo_titular']}','{row['decreto_nomeacao']}','{row['data_decreto']}','{row['nome_resp_setor']}','{tipo_comissao}','{row['portaria_comissao']}','{row['presid_comissao']}','{row['membro_com1']}','{row['cargo_membro1']}','{row['membro_com2']}','{row['cargo_membro2']}','{row['membro_com3']}','{row['cargo_membro3']}','{row['membro_com4']}','{row['cargo_membro4']}','{row['nome_diretor']}','{row['nome_secret']}',{row['resp_assina']},'{row['data_desig']}','{row['data_expira']}','{row['data_exonera']}','{row['descr_secret']}','{row['descr_diretor']}','{row['resp_padrao']}','{row['membro_com5']}','{row['cargo_membro5']}','{row['membro_com6']}','{row['cargo_membro6']}','{row['comissao_port_decr']}','{row['matr_secretario']}','{row['matr_diretor']}','{row['matr_resp_compras']}','{row['matr_pres_comissao']}','{row['matr_membro1']}','{row['matr_membro2']}','{row['matr_membro3']}','{row['matr_membro4']}','{row['matr_membro5']}','{row['matr_membro6']}','{row['nome_adv_resp']}','{row['oab_adv_resp']}',{row['cod_comissao']},'{row['cpf_presid']}','{row['cpf_membro1']}','{row['cpf_membro2']}','{row['cpf_membro3']}','{row['cpf_membro4']}','{row['cpf_membro5']}','{row['cpf_membro6']}','{row['descr_finalidade']}','{row['cpf_titular']}','{row['membro_com7']}','{row['cargo_membro7']}','{row['cpf_membro7']}','{row['matr_membro7']}','{row['membro_com8']}','{row['cargo_membro8']}','{row['cpf_membro8']}','{row['matr_membro8']}',{row['nat_cargo_presidente']},{row['nat_cargo1']},{row['nat_cargo2']},{row['nat_cargo3']},{row['nat_cargo4']},{row['nat_cargo5']},{row['nat_cargo6']},{row['nat_cargo7']},{row['nat_cargo8']},'{row['data_cadastro_tce']}',{row['i_entidades']},'{row['descr_natureza']}',{row['tipo_ordenador']},'{row['descr_natureza1']}','{row['descr_natureza2']}','{row['descr_natureza3']}','{row['descr_natureza4']}','{row['descr_natureza5']}','{row['descr_natureza6']}','{row['descr_natureza7']}','{row['descr_natureza8']}','{row['data_publ']}',{row['i_veiculo_publ']},'{row['cargo_presid']}','{row['documento_resp']}','{row['documento_presid']}','{row['documento_membro1']}','{row['documento_membro2']}','{row['documento_membro3']}','{row['documento_membro4']}','{row['documento_membro5']}','{row['documento_membro6']}','{row['documento_membro7']}','{row['documento_membro8']}','{row['email_titular']}');".replace(
                                "'None'", "null").replace("None", "null")

                            banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INCLUSAO", nomeOdbc=nomeOdbc, sistema="Compras",
                                                 tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                            df_filtrado = df.filter((pl.col("i_responsavel") == resp['i_responsavel']) & (pl.col("i_entidades") == resp['i_entidades']))

                            for processo in df_filtrado.iter_rows(named=True):
                                # Update
                                dadoAlterado.append(f"Alterada comissão do processo {processo['i_processo']}/{processo['i_ano_proc']} entidade {processo['i_entidades']} de {row['i_responsavel']} para {id}")
                                comandoUpdate += f"""update compras.processos set i_responsavel = {id} where i_processo = {processo['i_processo']} and i_ano_proc = {processo['i_ano_proc']} and i_responsavel = {processo['i_responsavel']} and i_entidades = {processo['i_entidades']};\n"""

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                                 tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_comissao_diferente_do_pregao: {e}")
            return

        if tipo_comissao_diferente_do_pregao:
            dado = analisa_tipo_comissao_diferente_do_pregao()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_comissao_diferente_do_pregao(listDados=dado)

    def analisa_corrige_comissao_sem_data_criacao_ou_designacao(pre_validacao):
        nomeValidacao = "Comissao não possui data de criação ou designação"

        def analisa_comissao_sem_data_criacao_ou_designacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_comissao_sem_data_criacao_ou_designacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select 
                                                r.i_responsavel, 
                                                isnull(dateformat(r.data_publ,'yyyy-mm-dd'),isnull(dateformat(r.data_desig,'yyyy-mm-dd'),'')) as dataCriacao,
                                                coalesce(
                                                    (select dateformat(min(p.data_processo),'yyyy-mm-dd')
                                                    from compras.processos p
                                                    where p.i_responsavel = r.i_responsavel 
                                                    and p.i_entidades = r.i_entidades),
                                                    dateformat(now() - 1825,'yyyy-mm-dd')
                                                ) as nova_dataCriacao,	
                                                r.i_entidades
                                            from compras.responsaveis r
                                            where dataCriacao = ''
                    """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de criacao ou designação no cadastro da comissao {row['nova_dataCriacao']} entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE compras.responsaveis set data_publ = '{row['nova_dataCriacao']}' where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_comissao_sem_data_criacao_ou_designacao: {e}")
            return

        if comissao_sem_data_criacao_ou_designacao:
            dado = analisa_comissao_sem_data_criacao_ou_designacao()

            if corrigirErros and len(dado) > 0:
                corrige_comissao_sem_data_criacao_ou_designacao(listDados=dado)

    def analisa_corrige_comissao_sem_responsavel_assinatura(pre_validacao):
        nomeValidacao = "A comissão não possui um responsavel pela assinatura"

        def analisa_comissao_sem_responsavel_assinatura():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_comissao_sem_responsavel_assinatura(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select 
                                                r.i_responsavel, 
                                                r.resp_assina,
                                                isnull(r.resp_assina,'') as responsavelAssinatura,
                                                r.i_entidades
                                            from compras.responsaveis r
                                            where responsavelAssinatura = 0  
                                        """)
                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        # 1 - Titular, 2 - Secr. Administ, 3 - Secr Finanças, 4-Diretor
                        dadoAlterado.append(f"Alterado responsavel pela assinatura da comissao {row['i_responsavel']} para 1-Titular")
                        comandoUpdate += f"""UPDATE compras.responsaveis set resp_assina = 1 where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_comissao_sem_responsavel_assinatura: {e}")
            return

        if comissao_sem_responsavel_assinatura:
            dado = analisa_comissao_sem_responsavel_assinatura()

            if corrigirErros and len(dado) > 0:
                corrige_comissao_sem_responsavel_assinatura(listDados=dado)

    def analisa_corrige_tipo_comissao_diferente_leiloeiro(pre_validacao):
        nomeValidacao = "Tipo de comissão diferente de Leiloeiro"

        def analisa_tipo_comissao_diferente_leiloeiro():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_comissao_diferente_leiloeiro(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select distinct
                                                r.i_responsavel, 
                                                r.tipo_comissao,
                                                p.modalidade,
                                                p.i_processo,
                                                p.i_ano_proc,
                                                case 
                                                    when (r.tipo_comissao in ('P', 'E', 'S')) and (p.modalidade in (7))  then isnull(p.i_processo,0)
                                                    else 0
                                                end as procTipoIncompativelLeilao,
                                                r.i_entidades
                                            from compras.responsaveis r,compras.processos p
                                            where r.i_responsavel = p.i_responsavel
                                            and r.i_entidades = p.i_entidades
                                            and procTipoIncompativelLeilao > 0
                                            """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df_resp = df.unique(subset=['i_responsavel', 'i_entidades'])
                    for resp in df_resp.iter_rows(named=True):
                        copy_responsavel = banco.consultar(f"select * from compras.responsaveis where i_responsavel = {resp['i_responsavel']} and i_entidades = {resp['i_entidades']};")
                        id_maximo = banco.consultar(f"select max(i_responsavel) as id from compras.responsaveis where i_entidades = {resp['i_entidades']};")
                        id = id_maximo[0]['id'] + 1
                        df_responsavel = pl.DataFrame(copy_responsavel)

                        for row in df_responsavel.iter_rows(named=True):
                            tipo_comissao = 'L'
                            # Insert
                            dadoInserido = []
                            dadoAlterado = []
                            dadoInserido.append(f"Realizado cadastro da comissão {id} com tipo_comissao L - Leiloeiro sendo igual a comissao {resp['i_responsavel']}")
                            comandoInsert = f"INSERT INTO compras.responsaveis VALUES({id},'{row['nome_titular']}','{row['cargo_titular']}','{row['decreto_nomeacao']}','{row['data_decreto']}','{row['nome_resp_setor']}','{tipo_comissao}','{row['portaria_comissao']}','{row['presid_comissao']}','{row['membro_com1']}','{row['cargo_membro1']}','{row['membro_com2']}','{row['cargo_membro2']}','{row['membro_com3']}','{row['cargo_membro3']}','{row['membro_com4']}','{row['cargo_membro4']}','{row['nome_diretor']}','{row['nome_secret']}',{row['resp_assina']},'{row['data_desig']}','{row['data_expira']}','{row['data_exonera']}','{row['descr_secret']}','{row['descr_diretor']}','{row['resp_padrao']}','{row['membro_com5']}','{row['cargo_membro5']}','{row['membro_com6']}','{row['cargo_membro6']}','{row['comissao_port_decr']}','{row['matr_secretario']}','{row['matr_diretor']}','{row['matr_resp_compras']}','{row['matr_pres_comissao']}','{row['matr_membro1']}','{row['matr_membro2']}','{row['matr_membro3']}','{row['matr_membro4']}','{row['matr_membro5']}','{row['matr_membro6']}','{row['nome_adv_resp']}','{row['oab_adv_resp']}',{row['cod_comissao']},'{row['cpf_presid']}','{row['cpf_membro1']}','{row['cpf_membro2']}','{row['cpf_membro3']}','{row['cpf_membro4']}','{row['cpf_membro5']}','{row['cpf_membro6']}','{row['descr_finalidade']}','{row['cpf_titular']}','{row['membro_com7']}','{row['cargo_membro7']}','{row['cpf_membro7']}','{row['matr_membro7']}','{row['membro_com8']}','{row['cargo_membro8']}','{row['cpf_membro8']}','{row['matr_membro8']}',{row['nat_cargo_presidente']},{row['nat_cargo1']},{row['nat_cargo2']},{row['nat_cargo3']},{row['nat_cargo4']},{row['nat_cargo5']},{row['nat_cargo6']},{row['nat_cargo7']},{row['nat_cargo8']},'{row['data_cadastro_tce']}',{row['i_entidades']},'{row['descr_natureza']}',{row['tipo_ordenador']},'{row['descr_natureza1']}','{row['descr_natureza2']}','{row['descr_natureza3']}','{row['descr_natureza4']}','{row['descr_natureza5']}','{row['descr_natureza6']}','{row['descr_natureza7']}','{row['descr_natureza8']}','{row['data_publ']}',{row['i_veiculo_publ']},'{row['cargo_presid']}','{row['documento_resp']}','{row['documento_presid']}','{row['documento_membro1']}','{row['documento_membro2']}','{row['documento_membro3']}','{row['documento_membro4']}','{row['documento_membro5']}','{row['documento_membro6']}','{row['documento_membro7']}','{row['documento_membro8']}','{row['email_titular']}');".replace(
                                "'None'", "null").replace("None", "null")

                            banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INCLUSAO", nomeOdbc=nomeOdbc, sistema="Compras",
                                                 tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                            df_filtrado = df.filter((pl.col("i_responsavel") == resp['i_responsavel']) & (pl.col("i_entidades") == resp['i_entidades']))

                            for processo in df_filtrado.iter_rows(named=True):
                                # Update
                                dadoAlterado.append(
                                    f"Alterada comissão do processo {processo['i_processo']}/{processo['i_ano_proc']} entidade {processo['i_entidades']} de {row['i_responsavel']} para {id}")
                                comandoUpdate += f"""update compras.processos set i_responsavel = {id} where i_processo = {processo['i_processo']} and i_ano_proc = {processo['i_ano_proc']} and i_responsavel = {processo['i_responsavel']} and i_entidades = {processo['i_entidades']};\n"""

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                                 tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_comissao_diferente_leiloeiro: {e}")
            return

        if tipo_comissao_diferente_leiloeiro:
            dado = analisa_tipo_comissao_diferente_leiloeiro()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_comissao_diferente_leiloeiro(listDados=dado)

    def analisa_corrige_data_exoneracao_inferior_data_processo(pre_validacao):
        nomeValidacao = "A exoneração da comissão não deve ser inferior aos processos atrelados"

        def analisa_data_exoneracao_inferior_data_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_exoneracao_inferior_data_processo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" --Valida se há comissões com data de exoneração anterior aos processos atrelados.
                                            SELECT 
                                                r.i_entidades, 
                                                r.i_responsavel, 
                                                data_exonera, 
                                                data_processo, 
                                                i_processo , 
                                                i_ano_proc
                                            FROM 
                                                compras.responsaveis r
                                                JOIN compras.processos p ON p.i_responsavel = r.i_responsavel AND p.i_entidades = r.i_entidades 
                                            WHERE 
                                                data_processo > data_exonera  AND data_exonera IS NOT NULL 
                                                ORDER BY 2
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.unique(subset=['i_responsavel', 'i_entidades'])

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de exoneração da comissão  {row['i_responsavel']} entidade {row['i_entidades']} para null")
                        comandoUpdate += f"""update compras.responsaveis set data_exonera = null where i_responsavel = {row['i_responsavel']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_exoneracao_inferior_data_processo: {e}")
            return

        if data_exoneracao_inferior_data_processo:
            dado = analisa_data_exoneracao_inferior_data_processo()

            if corrigirErros and len(dado) > 0:
                corrige_data_exoneracao_inferior_data_processo(listDados=dado)

    if dadosList:
        analisa_corrige_cpf_autoridade_invalido(pre_validacao="cpf_autoridade_invalido")
        analisa_corrige_cpf_presidente_invalido(pre_validacao="cpf_presidente_invalido")
        analisa_corrige_data_expiracao_nula(pre_validacao="data_expiracao_nula")
        analisa_corrige_tipo_comissao_diferente_do_pregao(pre_validacao="tipo_comissao_diferente_do_pregao")
        analisa_corrige_comissao_sem_data_criacao_ou_designacao(pre_validacao="comissao_sem_data_criacao_ou_designacao")
        analisa_corrige_comissao_sem_responsavel_assinatura(pre_validacao="comissao_sem_responsavel_assinatura")
        analisa_corrige_tipo_comissao_diferente_leiloeiro(pre_validacao="tipo_comissao_diferente_leiloeiro")
        analisa_corrige_data_exoneracao_inferior_data_processo(pre_validacao="data_exoneracao_inferior_data_processo")
