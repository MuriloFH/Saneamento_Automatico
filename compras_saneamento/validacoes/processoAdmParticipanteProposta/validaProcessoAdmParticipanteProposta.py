from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def processoAdmParticipanteProposta(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                                    corrigirErros=False,
                                    credor_com_itens_sem_proposta=False,
                                    proposta_participante_classificacao_invalida=False,
                                    divergencia_quantidade_participantes_com_e_sem_proposta=False,
                                    situacao_proposta_incorreta_para_processos_homologados=False,
                                    ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_credor_com_itens_sem_proposta(pre_validacao):
        nomeValidacao = "O Credor não possui proposta para todos os itens do processo."

        def analisa_credor_com_itens_sem_proposta():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_credor_com_itens_sem_proposta(listDados):
            tipoCorrecao = "INSERCAO"
            comandoInsert = ""
            dadoInserido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                       WITH cte AS (
                           SELECT DISTINCT 
                               cp.i_entidades,
                               cp.i_ano_proc,
                               cp.i_processo,
                               cp.i_credores
                           FROM compras.participantes cp
                           WHERE (
                               SELECT p1.soma 
                               FROM (
                                   SELECT sum(i_item) as soma, i_ano_proc, i_processo, i_entidades 
                                   FROM compras.itens_processo 
                                   GROUP BY i_ano_proc, i_processo, i_entidades
                               ) p1
                               WHERE p1.i_ano_proc = cp.i_ano_proc AND p1.i_processo = cp.i_processo 
                               AND p1.i_entidades = cp.i_entidades
                           ) > 
                           (
                               SELECT soma
                               FROM (
                                   SELECT sum(i_item) as soma, i_ano_proc, i_processo, i_credores, i_entidades
                                   FROM compras.participantes 
                                   GROUP BY i_ano_proc, i_processo, i_credores, i_entidades
                               ) cp1
                               WHERE cp1.i_ano_proc = cp.i_ano_proc 
                                   AND cp1.i_processo = cp.i_processo 
                                   AND cp1.i_entidades = cp.i_entidades 
                                   AND cp1.i_credores = cp.i_credores
                           ) 
                        )
                        SELECT cte.*, ip.i_item, ip.preco_max 
                        FROM cte
                        JOIN compras.itens_processo ip ON ip.i_ano_proc = cte.i_ano_proc 
                           AND ip.i_processo = cte.i_processo 
                           AND ip.i_entidades = cte.i_entidades
                           AND ip.i_item NOT IN (
                               SELECT cp.i_item 
                               FROM compras.participantes cp 
                               WHERE cp.i_ano_proc = ip.i_ano_proc 
                                   AND cp.i_processo = ip.i_processo 
                                   AND cp.i_entidades = ip.i_entidades 
                                   AND cp.i_credores = cte.i_credores
                           )
                   """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoInserido.append(f"Inserindo proposta para o item {row['i_item']} do credor {row['i_credores']}, processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']}")
                        comandoInsert += f"""INSERT INTO compras.participantes (i_ano_proc,i_processo,i_credores,i_item,preco_unit_part,preco_total,qtde_cotada,situacao,atual_objeto,nome_marca,ordem_clas,credenciado,vlr_descto,i_entidades,i_lotes,art_43_lcf_123_06,art_44_lcf_123_06,dt_descredencia,observacao,percent_bdi_tce,percent_encargo_tce) 
                        VALUES({row['i_ano_proc']},{row['i_processo']},{row['i_credores']},{row['i_item']},{row['preco_max']},0.00,0.000,4,NULL,NULL,1,1,0.0000,{row['i_entidades']},NULL,'A','A',NULL,NULL,NULL,NULL);\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro,
                                     preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_credor_com_itens_sem_proposta: {e}")
            return

        if credor_com_itens_sem_proposta:
            dado = analisa_credor_com_itens_sem_proposta()

            if corrigirErros and len(dado) > 0:
                corrige_credor_com_itens_sem_proposta(listDados=dado)

    def analisa_corrige_proposta_participante_classificacao_invalida(pre_validacao):
        nomeValidacao = "No processo administrativo a proposta do participante e item possui classificação inválida!"

        def analisa_proposta_participante_classificacao_invalida():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_proposta_participante_classificacao_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    with cte as (
                        SELECT 
                            participantes.i_entidades,
                            participantes.i_ano_proc,
                            participantes.i_processo,
                            participantes.i_credores,
                            participantes.i_item,
                            dateformat(isnull(data_homolog,'1900-01-01'),'yyyy-mm-dd') as homologacao,
                            processos.modalidade,
                            participantes.situacao 
                        from compras.participantes, compras.processos
                        where participantes.i_entidades = processos.i_entidades and participantes.i_processo = processos.i_processo and participantes.i_ano_proc = processos.i_ano_proc

                        UNION ALL 

                        select distinct
                            processos.i_entidades,
                            processos.i_ano_proc,
                            processos.i_processo,
                            participantes_processos.i_credores,
                            itens_processo.i_item,
                            dateformat(isnull(processos.data_homolog,'1900-01-01'),'yyyy-mm-dd') as homologacao,
                            processos.modalidade,
                            participantes.situacao
                        FROM compras.itens_processo 
                        JOIN compras.processos ON (itens_processo.i_ano_proc = processos.i_ano_proc and itens_processo.i_processo = processos.i_processo and itens_processo.i_entidades = processos.i_entidades) 
                        JOIN compras.participantes_processos on (participantes_processos.i_ano_proc = processos.i_ano_proc and participantes_processos.i_processo = processos.i_processo and participantes_processos.i_entidades = processos.i_entidades) 
                        JOIN compras.forma_julg ON (processos.i_forma_julg = forma_julg.i_forma_julg and processos.i_entidades = forma_julg.i_entidades)
                        join compras.participantes  on participantes.i_entidades = processos.i_entidades and participantes.i_processo = processos.i_processo and participantes.i_ano_proc = processos.i_ano_proc 
                        WHERE forma_julg.forma_especial = 4
                        and itens_processo.i_item NOT IN (SELECT p.i_item
                                                             FROM compras.participantes p
                                                             WHERE p.i_entidades = participantes_processos.i_entidades and p.i_ano_proc = participantes_processos.i_ano_proc and p.i_processo = participantes_processos.i_processo and p.i_credores = participantes_processos.i_credores)
                    )select 
                        *
                    from 
                        cte
                    where cte.modalidade in (13,14)
                    and cte.homologacao <> '1900-01-01'
                    and cte.situacao not in (1,2,3,4,5,6,8,9)
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada situacao do participante {row['i_credores']} do processo {row['i_processo']}/{row['i_ano_proc']} item {row['i_item']} para 4 - Nao Cotou")
                        comandoUpdate += f"""UPDATE compras.participantes set situacao = 4 where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_item = {row['i_item']} and i_credores = {row['i_credores']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_proposta_participante_classificacao_invalida: {e}")
            return

        if proposta_participante_classificacao_invalida:
            dado = analisa_proposta_participante_classificacao_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_proposta_participante_classificacao_invalida(listDados=dado)

    def analisa_corrige_divergencia_quantidade_participantes_com_e_sem_proposta(pre_validacao):
        nomeValidacao = "No processo administrativo a proposta do participante e item possui classificação inválida!"

        def analisa_divergencia_quantidade_participantes_com_e_sem_proposta():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_divergencia_quantidade_participantes_com_e_sem_proposta(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoDelete = ""
            comandoInsert = ""
            dadoDeletado = []
            dadoInserido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT 
                        p.i_entidades, 
                        p.i_processo, 
                        p.i_ano_proc,
                        prt.countParticipantes,
                        prp.countPropostas
                    FROM 
                        compras.processos p 
                        INNER JOIN (SELECT
                                        i_ano_proc,
                                        i_processo,
                                        i_entidades, 
                                        COUNT(DISTINCT i_credores) countPropostas 
                                    FROM 
                                        compras.participantes
                                    GROUP BY 
                                        i_ano_proc,
                                        i_processo,
                                        i_entidades) prp
                        ON p.i_ano_proc = prp.i_ano_proc
                        AND p.i_processo = prp.i_processo
                        AND p.i_entidades = prp.i_entidades
                        INNER JOIN (SELECT
                                        i_ano_proc,
                                        i_processo,
                                        i_entidades,
                                        COUNT(*) countParticipantes
                                    FROM
                                        compras.participantes_processos
                                    GROUP BY 
                                        i_ano_proc,
                                        i_processo,
                                        i_entidades) prt
                        ON p.i_ano_proc = prt.i_ano_proc
                        AND p.i_processo = prt.i_processo
                        AND p.i_entidades = prt.i_entidades
                    WHERE prt.countParticipantes <> prp.countPropostas
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        if row['countParticipantes'] < row['countPropostas']:
                            busca2 = banco.consultar(f"""select distinct p.i_credores 
                                                        from compras.participantes p
                                                        where p.i_credores not in (select i_credores from compras.participantes_processos pp where pp.i_ano_proc = p.i_ano_proc and pp.i_processo = p.i_processo and pp.i_entidades = p.i_entidades )
                                                        and p.i_ano_proc = {row['i_ano_proc']} and p.i_processo = {row['i_processo']} and p.i_entidades = {row['i_entidades']}
                            """)

                            for credor in busca2:
                                dadoInserido.append(f"Inserido credor {credor['i_credores']} como participante no processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']}")
                                comandoInsert += f"""INSERT INTO compras.compras.participantes_processos(i_ano_proc, i_entidades, i_processo, i_credores, credenciado, representante, cpf_representante, renuncia_recurso, observacoes, enquadra_lcf123, mpe_local_reg) VALUES({row['i_ano_proc']}, {row['i_entidades']}, {row['i_processo']}, {credor['i_credores']}, 'N', '', '', 'S', NULL, 'N', 'N');\n"""

                        else:
                            busca2 = banco.consultar(f"""select DISTINCT p.i_credores 
                                                        from compras.participantes_processos p
                                                        where p.i_credores not in (select i_credores from compras.participantes pp where pp.i_ano_proc = p.i_ano_proc and pp.i_processo = p.i_processo and pp.i_entidades = p.i_entidades)
                                                        and p.i_ano_proc = {row['i_ano_proc']} and p.i_processo = {row['i_processo']} and p.i_entidades = {row['i_entidades']}
                            """)
                            for credor in busca2:
                                dadoDeletado.append(f"Excluído credor {credor['i_credores']} processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']} devido ausencia de itens")
                                comandoDelete += f"""DELETE FROM compras.compras.participantes_processos WHERE i_credores= {credor['i_credores']} and i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao="INSERÇÃO", nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro,
                                     preValidacaoBanco=pre_validacao, dadoAlterado=dadoInserido)

                banco.executarComLog(comando=banco.triggerOff(comandoDelete), logAlteracoes=logAlteracoes, tipoCorrecao="EXCLUSÃO", nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro,
                                     preValidacaoBanco=pre_validacao, dadoAlterado=dadoDeletado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_divergencia_quantidade_participantes_com_e_sem_proposta: {e}")
            return

        if divergencia_quantidade_participantes_com_e_sem_proposta:
            dado = analisa_divergencia_quantidade_participantes_com_e_sem_proposta()

            if corrigirErros and len(dado) > 0:
                corrige_divergencia_quantidade_participantes_com_e_sem_proposta(listDados=dado)

    def analisa_corrige_situacao_proposta_incorreta_para_processos_homologados(pre_validacao):
        nomeValidacao = "Proposta com situação incorreta em um processo homologado"

        def analisa_situacao_proposta_incorreta_para_processos_homologados():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_situacao_proposta_incorreta_para_processos_homologados(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select pa.i_processo as i_processo,
                                               pa.i_ano_proc as i_ano_proc,
                                               pa.i_credores,
                                               pa.i_item,
                                               pa.situacao,
                                               pa.i_entidades as i_entidades
                                            from
                                               compras.participantes pa join compras.processos p
                                               on (pa.i_processo = p.i_processo and
                                               pa.i_ano_proc = p.i_ano_proc and
                                               pa.i_entidades = p.i_entidades)
                                            where
                                               pa.situacao in (0, 5, 7)
                                               and pa.i_ano_proc >= 1900
                                               and pa.i_entidades = pa.i_entidades
                                               and p.data_homolog is not null
                                            order by pa.i_ano_proc, pa.i_processo, pa.i_credores, pa.i_item
                                           """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.participantes p set situacao = 1 where p.i_processo = {row['i_processo']} and p.i_ano_proc = {row['i_ano_proc']} and i_entidades = {row['i_entidades']} and i_credores = {row['i_credores']} and i_item = {row['i_item']} and situacao = {row['situacao']};\n"""
                        dadoAlterado.append(f"Ajustado a situação do credor {row['i_credores']} para 1 do processo {row['i_processo']}/{row['i_ano_proc']} no item {row['i_item']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro,
                                     preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_situacao_proposta_incorreta_para_processos_homologados: {e}")
            return

        if situacao_proposta_incorreta_para_processos_homologados:
            dado = analisa_situacao_proposta_incorreta_para_processos_homologados()

            if corrigirErros and len(dado) > 0:
                corrige_situacao_proposta_incorreta_para_processos_homologados(listDados=dado)

    if dadosList:
        analisa_corrige_credor_com_itens_sem_proposta(pre_validacao="credor_com_itens_sem_proposta")
        analisa_corrige_proposta_participante_classificacao_invalida(pre_validacao="proposta_participante_classificacao_invalida")
        analisa_corrige_divergencia_quantidade_participantes_com_e_sem_proposta(pre_validacao="divergencia_quantidade_participantes_com_e_sem_proposta")
        analisa_corrige_situacao_proposta_incorreta_para_processos_homologados(pre_validacao='situacao_proposta_incorreta_para_processos_homologados')
