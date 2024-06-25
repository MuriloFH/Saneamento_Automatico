from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def processoAdmAtoFinal(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        data_anulacao_revogacao_processo_anterior_data_homologacao=False,
                        responsavel_anulacao_nao_informado=False,
                        processo_homologado_sem_participante_vencedor=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_anulacao_revogacao_processo_anterior_data_homologacao(pre_validacao):
        nomeValidacao = "A anulação/revogação do processo está com data anterior a data de homologação"

        def analisa_data_anulacao_revogacao_processo_anterior_data_homologacao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_anulacao_revogacao_processo_anterior_data_homologacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    select pr.i_ano_proc , pr.i_processo , pr.i_entidades , pr.data_homolog ,ap.i_anl_processo , ap.data_anulacao , ap.i_ano_anl_proc,
                    case 
                        when ap.i_ano_anl_proc > pr.i_ano_proc then ap.i_ano_anl_proc || RIGHT(ap.data_anulacao, 6)
                        else ap.i_ano_proc || substring(pr.data_homolog,5,4) || cast(RIGHT(pr.data_homolog,2) + 1 as int)
                    end as nova_data
                    from compras.processos pr 
                    left join compras.anl_processos ap on pr.i_processo = ap.i_processo and pr.i_ano_proc = ap.i_ano_proc 
                    where pr.data_homolog is not null
                    and data_anulacao <= data_homolog
                    and exists(select ano_exerc from compras.parametros_anuais where parametros_anuais.i_entidades = 1 and parametros_anuais.ano_exerc = pr.i_ano_proc)

                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data de anulação/revogação do processo {row['i_processo']}/{row['i_ano_proc']} para {row['nova_data']}")
                        comandoUpdate += f"""UPDATE compras.anl_processos set data_anulacao = '{row['nova_data']}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_anl_processo = {row['i_anl_processo']} and i_ano_anl_proc = {row['i_ano_anl_proc']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_anulacao_revogacao_processo_anterior_data_homologacao: {e}")
            return

        if data_anulacao_revogacao_processo_anterior_data_homologacao:
            dado = analisa_data_anulacao_revogacao_processo_anterior_data_homologacao()

            if corrigirErros and len(dado) > 0:
                corrige_data_anulacao_revogacao_processo_anterior_data_homologacao(listDados=dado)

    def analisa_corrige_responsavel_anulacao_nao_informado(pre_validacao):
        nomeValidacao = "O responsável pela anulação do processo não foi informado"

        def analisa_responsavel_anulacao_nao_informado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_responsavel_anulacao_nao_informado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            dadoInserido = []
            comandoInsert = ""

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    with cte_proc as (
                        select pr.i_ano_proc , pr.i_processo , pr.i_entidades , pr.i_responsavel , ap.i_anl_processo , ap.data_anulacao , ap.i_ano_anl_proc,
                        ap.i_responsaveis_atos 
                        from compras.processos pr 
                        join compras.anl_processos ap on pr.i_processo = ap.i_processo and pr.i_ano_proc = ap.i_ano_proc 
                        where ap.i_responsaveis_atos is null
                    )
                    ,cte_resp as (
                        select 
                        ROW_NUMBER() over(partition by r.i_responsavel order by r.i_responsavel) as contador,
                        r.i_responsavel, 
                        r.nome_titular, 
                        r.cpf_titular,
                        r.i_entidades,
                        ra.i_responsaveis_atos
                        from compras.responsaveis r 
                        left join compras.responsaveis_atos ra on ra.cpf = r.cpf_titular
                    )select 
                        p.i_ano_proc , p.i_processo , p.i_entidades, p.i_anl_processo, r.i_responsavel, r.nome_titular, r.cpf_titular, r.i_responsaveis_atos
                    from 
                        cte_proc p
                    join cte_resp r on r.i_responsavel = p.i_responsavel and r.i_entidades = p.i_entidades
                    where contador = 1
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        if row['i_responsaveis_atos']:
                            dadoAlterado.append(f"Alterado responsavel pela anulação do processo {row['i_processo']}/{row['i_ano_proc']} de null para {row['i_responsaveis_atos']}")
                            comandoUpdate += f"""UPDATE compras.anl_processos set i_responsaveis_atos = {row['i_responsaveis_atos']} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_anl_processo = {row['i_anl_processo']} and i_entidades = {row['i_entidades']};\n"""
                        else:
                            max_resp = banco.consultar("select max(i_responsaveis_atos) + 1 as max from compras.responsaveis_atos")
                            novo_id = max_resp[0]['max']
                            dadoInserido.append(f"Inserido responsavel ato {novo_id}")
                            comandoInsert += f"""INSERT INTO compras.responsaveis_atos (i_responsaveis_atos, nome, cpf, i_entidades) VALUES ({novo_id},'{row['nome_titular']}','{row['cpf_titular']}',{row['i_entidades']});\n"""
                            banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INSERCAO", nomeOdbc=nomeOdbc, sistema="Compras",
                                                 tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                            dadoAlterado.append(f"Alterado responsavel pela anulação do processo {row['i_processo']}/{row['i_ano_proc']} de null para {novo_id}")
                            comandoUpdate += f"""UPDATE compras.anl_processos set i_responsaveis_atos = {novo_id} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_anl_processo = {row['i_anl_processo']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_responsavel_anulacao_nao_informado: {e}")
            return

        if responsavel_anulacao_nao_informado:
            dado = analisa_responsavel_anulacao_nao_informado()

            if corrigirErros and len(dado) > 0:
                corrige_responsavel_anulacao_nao_informado(listDados=dado)

    def analisa_corrige_processo_homologado_sem_participante_vencedor(pre_validacao):
        nomeValidacao = "O processo possui homologação, porém não há nenhum participante vencedor para o processo."

        def analisa_processo_homologado_sem_participante_vencedor():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_homologado_sem_participante_vencedor(listDados):
            tipoCorrecao = "ALTERACAO"

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                                            SELECT p.i_ano_proc,
                                                p.i_processo,
                                                p.i_entidades,
                                                dateformat(p.data_homolog + 1,'yyyy-mm-dd') as data_anul,
                                                (SELECT count(*)
                                                FROM compras.processos
                                                INNER JOIN compras.participantes ON participantes.i_ano_proc = processos.i_ano_proc
                                                AND participantes.i_processo = processos.i_processo
                                                AND participantes.i_entidades = processos.i_entidades
                                                WHERE participantes.situacao IN (2, 5, 6, 7, 10) -- considera credenciados, vencedores, empates e ex-vencedores
                                                    AND processos.i_ano_proc = p.i_ano_proc
                                                    AND processos.i_processo = p.i_processo
                                                    AND processos.i_entidades = p.i_entidades) AS qtd_vencedores,
                                                (SELECT 1 FROM compras.anl_processos anl
                                                    WHERE anl.i_ano_proc = p.i_ano_proc
                                                    AND anl.i_processo = p.i_processo
                                                    AND anl.i_entidades = p.i_entidades) AS anulacoes,
                                                (select count(*) from compras.coletas c where c.i_ano_proc = p.i_ano_proc and c.i_processo = p.i_processo and c.i_entidades = p.i_entidades) as possui_ata,
                                                (select count(*) from compras.contratos c where c.i_ano_proc = p.i_ano_proc and c.i_processo = p.i_processo and c.i_entidades = p.i_entidades) as possui_contrato,
                                                (select COUNT(*) from compras.sequ_autor sf where sf.i_ano_proc = p.i_ano_proc and sf.i_processo = p.i_processo and sf.i_entidades = p.i_entidades) as possui_sf
                                                FROM compras.processos p
                                                WHERE anulacoes IS NULL
                                                AND data_homolog IS NOT NULL
                                                AND qtd_vencedores = 0
                                        """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        # Solução para quando não possuir ata, contrato e solicitação de fornecimento será inserir uma anulação para o processo sem participante vencedor
                        if row['possui_ata'] == 0 and row['possui_contrato'] == 0 and row['possui_sf'] == 0:
                            busca_resp = banco.consultar(f"""
                                with cte_proc as (
                                    select pr.i_ano_proc , pr.i_processo , pr.i_entidades , pr.i_responsavel, pr.data_homolog
                                    from compras.processos pr 
                                    where pr.i_ano_proc = {row['i_ano_proc']}
                                    and pr.i_processo = {row['i_processo']}
                                    and pr.i_entidades = {row['i_entidades']}
                                )
                                ,cte_resp as (
                                    select 
                                        r.i_responsavel, 
                                        r.nome_titular, 
                                        r.cpf_titular,
                                        r.i_entidades,
                                        ra.i_responsaveis_atos
                                        from compras.responsaveis r 
                                        left join compras.responsaveis_atos ra on ra.cpf = r.cpf_titular
                                )select distinct
                                    p.i_ano_proc , p.i_processo , p.i_entidades, r.i_responsavel, r.nome_titular, r.cpf_titular, r.i_responsaveis_atos
                                from 
                                    cte_proc p
                                join cte_resp r on r.i_responsavel = p.i_responsavel and r.i_entidades = p.i_entidades
                            """)

                            if (busca_resp[0]['i_responsaveis_atos']):
                                busca_max_anl = banco.consultar(f"""
                                    select COALESCE (max(i_anl_processo) + 1,1) as max_anl_proc 
                                    from compras.anl_processos
                                    where i_ano_proc = {row['i_ano_proc']}
                                """)

                                dadoInserido = [f"Inserida anulação do processo {row['i_processo']}/{row['i_ano_proc']}"]
                                comandoInsert = f"""INSERT INTO compras.anl_processos (i_ano_proc, i_anl_processo, i_processo, data_anulacao, motivo, anul_revog, i_ano_anl_proc, i_entidades, i_responsaveis_atos, finalizar_situacao) VALUES({row['i_ano_proc']}, {busca_max_anl[0]['max_anl_proc']}, {row['i_processo']}, '{row['data_anul']}', 'AUSÊNCIA PARTICIPANTE VENCEDOR', 'D', {row['i_ano_proc']}, {row['i_entidades']}, {busca_resp[0]['i_responsaveis_atos']}, 'N');\n"""
                                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INSERCAO", nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)
                            else:
                                max_resp = banco.consultar("select max(i_responsaveis_atos) + 1 as max from compras.responsaveis_atos")
                                novo_id = max_resp[0]['max']
                                dadoInserido = [f"Inserido responsavel ato {novo_id}"]
                                comandoInsert = f"""INSERT INTO compras.responsaveis_atos (i_responsaveis_atos, nome, cpf, i_entidades) VALUES ({novo_id},'{busca_resp[0]['nome_titular']}','{busca_resp[0]['cpf_titular']}',{busca_resp[0]['i_entidades']});\n"""
                                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INSERCAO", nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                                dadoInserido = [f"Inserida anulação do processo {row['i_processo']}/{row['i_ano_proc']}"]
                                comandoInsert = f"""INSERT INTO compras.anl_processos (i_ano_proc, i_anl_processo, i_processo, data_anulacao, motivo, anul_revog, i_ano_anl_proc, i_entidades, i_responsaveis_atos, finalizar_situacao) VALUES({row['i_ano_proc']}, 1, {row['i_processo']}, '{row['data_anul']}', 'AUSÊNCIA PARTICIPANTE VENCEDOR', 'D', {row['i_ano_proc']}, {row['i_entidades']}, {novo_id}, 'N');\n"""
                                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INSERCAO", nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                        if row['possui_ata'] != 0:
                            dadoAlterado = []
                            comandoUpdate = ""
                            busca_ata = banco.consultar(f"""
                                                            select c.i_ano_coleta, c.i_coleta, c.i_entidades, c.i_ano_proc, c.i_processo, ic.i_credores, ic.i_item_coleta, ic.i_material, ic.preco_total_coleta,
                                                            (select p.situacao from compras.participantes p where p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo and p.i_credores = ic.i_credores and p.i_entidades = c.i_entidades and p.i_item = ic.i_item_coleta ) as situacao
                                                            from compras.coletas c
                                                            join compras.itens_coletas ic on ic.i_ano_coleta = c.i_ano_coleta and ic.i_coleta = c.i_coleta and ic.i_entidades = c.i_entidades
                                                            where ic.comprar_item = 'S'
                                                            and c.i_ano_proc = {row['i_ano_proc']}
                                                            and c.i_processo = {row['i_processo']}
                                                            and c.i_entidades = {row['i_entidades']}
                                                            order by ic.i_credores
                                                        """)
                            df_ata = pl.DataFrame(busca_ata)
                            for ata in df_ata.iter_rows(named=True):
                                if ata['situacao'] not in (2, 5, 6, 7, 10):
                                    dadoAlterado.append(f"Alterada situacao do processo {ata['i_processo']}/{ata['i_ano_proc']} credor {ata['i_credores']} item {ata['i_item_coleta']} da ata {ata['i_coleta']}/{ata['i_ano_coleta']}")
                                    comandoUpdate += f"""UPDATE compras.participantes set situacao = 2 where i_ano_proc = {ata['i_ano_proc']} and i_processo = {ata['i_processo']} and i_entidades = {ata['i_entidades']} and i_credores = {ata['i_credores']} and i_item = {ata['i_item_coleta']} and situacao not IN (2, 5, 6, 7, 10);\n"""
                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                        if row['possui_contrato'] != 0:
                            dadoAlterado = []
                            comandoUpdate = ""
                            busca_contr = banco.consultar(f"""
                                select c.i_contratos, c.i_entidades, c.i_ano_proc, c.i_processo, c.i_credores, ic.i_item_ctr , ic.i_material, ic.preco_total_ctr,
                                (select p.situacao from compras.participantes p where p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo and p.i_credores = c.i_credores and p.i_entidades = c.i_entidades and p.i_item = ic.i_item_ctr ) as situacao
                                from compras.contratos c
                                left join compras.contratos_itens ic on ic.i_ano_proc = c.i_ano_proc and ic.i_processo = c.i_processo and ic.i_entidades = c.i_entidades and ic.i_contratos = c.i_contratos
                                where c.i_ano_proc = {row['i_ano_proc']}
                                and c.i_processo = {row['i_processo']}
                                and c.i_entidades = {row['i_entidades']}
                                order by c.i_credores
                            """)
                            df_contr = pl.DataFrame(busca_contr)
                            for contr in df_contr.iter_rows(named=True):
                                if contr['situacao'] and contr['situacao'] not in (2, 5, 6, 7, 10):
                                    dadoAlterado.append(f"Alterada situacao do processo {contr['i_processo']}/{contr['i_ano_proc']} credor {contr['i_credores']} item {contr['i_item_ctr']} do contrato {contr['i_contratos']}")
                                    comandoUpdate += f"""UPDATE compras.participantes set situacao = 2 where i_ano_proc = {contr['i_ano_proc']} and i_processo = {contr['i_processo']} and i_entidades = {contr['i_entidades']} and i_credores = {contr['i_credores']} and i_item = {contr['i_item_ctr']} and situacao not IN (2, 5, 6, 7, 10);\n"""
                                elif not contr['situacao'] and row['possui_ata'] == 0 and row['possui_sf'] == 0:
                                    # Retirando a data de Homologação do Processo
                                    dadoAlterado.append(f"Alterada data de homologação do processo {contr['i_processo']}/{contr['i_ano_proc']} para nulo")
                                    comandoUpdate += f"""UPDATE compras.compras.processos SET data_homolog = null WHERE i_ano_proc={contr['i_ano_proc']} and i_processo = {contr['i_processo']} and i_entidades = {contr['i_entidades']};\n"""

                                    #Desvinculando processo do contrato
                                    dadoAlterado.append(f"Alterado contrato {contr['i_contratos']} do credor  {contr['i_credores']} processo {contr['i_processo']}/{contr['i_ano_proc']} retirando vinculo de processo e ano")
                                    comandoUpdate += f"""UPDATE compras.compras.contratos SET i_ano_proc = null, i_processo = null WHERE i_contratos = {contr['i_contratos']} and i_ano_proc = {contr['i_ano_proc']} and i_processo = {contr['i_processo']} and i_credores = {contr['i_credores']} and i_entidades = {contr['i_entidades']};\n"""

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                        if row['possui_sf'] != 0:
                            dadoAlterado = []
                            comandoUpdate = ""
                            busca_sf = banco.consultar(f"""
                                                            select distinct hp.i_ano_proc, hp.i_processo , hp.i_entidades, hp.i_credores , hp.i_item,
                                                            (select p.situacao from compras.participantes p where p.i_ano_proc = hp.i_ano_proc and p.i_processo = hp.i_processo and p.i_credores = hp.i_credores and p.i_entidades = hp.i_entidades and p.i_item = hp.i_item ) as situacao
                                                            from compras.homologa_parcial hp
                                                            where hp.i_ano_proc = {row['i_ano_proc']}
                                                            and hp.i_processo = {row['i_processo']}
                                                            and hp.i_entidades = {row['i_entidades']}
                                                            order by hp.i_credores 
                                                        """)
                            df_sf = pl.DataFrame(busca_sf)
                            for sf in df_sf.iter_rows(named=True):
                                if sf['situacao'] not in (2, 5, 6, 7, 10):
                                    dadoAlterado.append(f"Alterada situacao do processo {sf['i_processo']}/{sf['i_ano_proc']} credor {sf['i_credores']} item {sf['i_item']} ")
                                    comandoUpdate += f"""UPDATE compras.participantes set situacao = 2 where i_ano_proc = {sf['i_ano_proc']} and i_processo = {sf['i_processo']} and i_entidades = {sf['i_entidades']} and i_credores = {sf['i_credores']} and i_item = {sf['i_item']} and situacao not IN (2, 5, 6, 7, 10);\n"""

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_homologado_sem_participante_vencedor: {e}")
            return

        if processo_homologado_sem_participante_vencedor:
            dado = analisa_processo_homologado_sem_participante_vencedor()

            if corrigirErros and len(dado) > 0:
                corrige_processo_homologado_sem_participante_vencedor(listDados=dado)

    if dadosList:
        analisa_corrige_data_anulacao_revogacao_processo_anterior_data_homologacao(pre_validacao="data_anulacao_revogacao_processo_anterior_data_homologacao")
        analisa_corrige_responsavel_anulacao_nao_informado(pre_validacao="responsavel_anulacao_nao_informado")
        analisa_corrige_processo_homologado_sem_participante_vencedor(pre_validacao="processo_homologado_sem_participante_vencedor")
