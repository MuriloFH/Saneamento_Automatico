import math

from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def solicitacaoFornecimentoItem(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                                corrigirErros=False,
                                item_nao_presente_no_contrato=False,
                                qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato=False
                                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_item_nao_presente_no_contrato(pre_validacao):
        nomeValidacao = "Item da SF não localizado no contrato vinculado"

        def analisa_item_nao_presente_no_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            if dadosList:
                for i in dadosList:
                    if i['pre_validacao'] == f'{pre_validacao}':
                        dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_item_nao_presente_no_contrato(listDados):
            tipoCorrecao = "INSERSAO"
            comandoUpdate = ""
            comandoInsert = ""
            itemAtual = 0
            somaQtdItensCredor = 0
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""SELECT distinct
                                             hp.i_entidades_partic,
                                             hp.i_entidades,
                                             hp.i_ano_proc,
                                             hp.i_processo,
                                             hp.i_credores,
                                             hp.i_sequ_adj,
                                             hp.i_item,
                                             isnull(hp.i_ano_desp,0) as i_ano_desp,
                                             isnull(hp.i_despesas,0) as i_despesas,
                                             codContratoAF = isnull((Select min(af.i_contratos) from compras.sequ_autor af
                                                                     where af.i_entidades = hp.i_entidades
                                                                       and af.i_entidades_partic  = hp.i_entidades_partic
                                                                       and af.i_credores  = hp.i_credores
                                                                       and af.i_processo  = hp.i_processo
                                                                       and af.i_ano_proc  = hp.i_ano_proc
                                                                       and af.i_sequ_adj  = hp.i_sequ_adj),0),

                                             existeContrato = if codContratoAF > 0 then
                                                                 codContratoAF
                                                              else
                                                                 isnull((select min(ct.i_contratos) from compras.contratos ct
                                                                       where ct.i_entidades = hp.i_entidades_partic
                                                                         and ct.i_credores  = hp.i_credores
                                                                         and ct.i_processo  = hp.i_processo
                                                                         and ct.i_ano_proc  = hp.i_ano_proc),0)
                                                              endif,
                                             data_ass_contrato = if existeContrato > 0 then
                                                                    (select ct.data_ass from compras.contratos ct
                                                                             where ct.i_entidades  = hp.i_entidades_partic
                                                                               and ct.i_contratos  = existeContrato)
                                                                 endif,

                                             data_inicio_vigencia = if existeContrato > 0 then
                                                                       (select ct.data_ini_vig from compras.contratos ct
                                                                              where ct.i_entidades  = hp.i_entidades_partic
                                                                                and ct.i_contratos  = existeContrato)
                                                                    endif,

                                             data_SF = if isnull(adj.data_af, adj.data_adjudica) < isnull(data_inicio_vigencia, '1900-01-01') then
                                              dateformat(data_inicio_vigencia,'yyyy-mm-dd')
                                               else
                                                  if isnull(adj.data_af, adj.data_adjudica) < isnull(data_ass_contrato, '1900-01-01') then
                                                 dateformat(data_ass_contrato,'yyyy-mm-dd')
                                              else
                                                 dateformat(isnull(adj.data_af, adj.data_adjudica),'yyyy-mm-dd')
                                                          endif
                                                       endif,
                                             ano_SF = year(data_af),

                                             solicitacaoFornec = trim(str(isnull((select first s.i_sequ_aut
                                                      from compras.sequ_autor s
                                                     where s.i_entidades = hp.i_entidades_partic
                                                       and s.i_processo = hp.i_processo
                                                       and s.i_ano_proc = hp.i_ano_proc
                                                       and s.i_sequ_adj = hp.i_sequ_adj
                                                       and s.i_sequ_adj = hp.i_sequ_adj
                                                       and s.i_credores = hp.i_credores
                                                     order by s.i_sequ_aut), ''))),

                                             itemInclusoContrato = if existeContrato > 0 then
                                                                  isnull((select first 1 from compras.contratos_itens ci where ci.i_entidades = hp.i_entidades_partic and ci.i_contratos = existeContrato and ci.i_item_ctr = hp.i_item),'')
                                                               else
                                                                  1
                                                               endif,            
                                            quantidade = (select cast(sum(isnull(h.qtde_parcial,0)) as numeric(14,3)) from compras.homologa_parcial h where hp.i_entidades_partic = h.i_entidades_partic
                                                            and h.i_entidades = hp.i_entidades
                                                            and h.i_entidades_partic = hp.i_entidades_partic
                                                            and h.i_ano_proc  = hp.i_ano_proc
                                                            and h.i_processo = hp.i_processo
                                                            and h.i_sequ_adj = hp.i_sequ_adj
                                                            and h.i_item = hp.i_item
                                                            and h.i_credores = hp.i_credores
                                                            and isnull(h.i_ano_desp, 0) = isnull(hp.i_ano_desp, 0)
                                                            and isnull(h.i_despesas, 0) = isnull(hp.i_despesas, 0)
                                                            ),
                                            valorTotal = (select cast(sum(h.preco_total_adj) as numeric(13,2)) from compras.homologa_parcial h where hp.i_entidades_partic = h.i_entidades_partic
                                                            and h.i_entidades = hp.i_entidades
                                                            and h.i_entidades_partic = hp.i_entidades_partic
                                                            and h.i_ano_proc  = hp.i_ano_proc
                                                            and h.i_processo = hp.i_processo
                                                            and h.i_sequ_adj = hp.i_sequ_adj
                                                            and h.i_item = hp.i_item
                                                            and h.i_credores = hp.i_credores
                                                            and isnull(h.i_ano_desp, 0) = isnull(hp.i_ano_desp, 0)
                                                            and isnull(h.i_despesas, 0) = isnull(hp.i_despesas, 0)
                                                            )
                                               from compras.homologa_parcial hp
                                                    join compras.adjudicacao adj on (adj.i_entidades = hp.i_entidades and adj.i_entidades_partic = hp.i_entidades_partic and adj.i_ano_proc  = hp.i_ano_proc and adj.i_processo = hp.i_processo and adj.i_sequ_adj = hp.i_sequ_adj)
                                                    join compras.processos on (hp.i_entidades = processos.i_entidades and hp.i_ano_proc = processos.i_ano_proc and hp.i_processo = processos.i_processo)
                                              where solicitacaoFornec > 0
                                                and year(data_SF) >= 1900 
                                                and existeContrato > 0
                                                and itemInclusoContrato = 0
                                            order by existeContrato, hp.i_ano_proc, hp.i_processo, i_item asc
                                        """)

                for i in dados:
                    if i['codContratoAF'] == 0:
                        i['codContratoAF'] = i['existeContrato']

                    if itemAtual != int(i['i_item']):
                        itemAtual = int(i['i_item'])

                        # passo 1 - descobrir se tem credor do tipo 6 para o item da SF
                        temCredorTipo6 = banco.consultar(f"""SELECT p.i_credores
                                                                from compras.compras.participantes p 
                                                                where p.i_ano_proc = {i['i_ano_proc']}
                                                                and p.i_processo = {i['i_processo']}
                                                                and p.i_item = {i['i_item']}
                                                                and p.i_entidades = {i['i_entidades']}
                                                                and situacao = 6;
                                                        """)
                        # passo 1.1 - se retornar dados, devo procurar pelos credores que retornou
                        if len(temCredorTipo6) > 0:
                            listCredores = []
                            for credor in temCredorTipo6:
                                listCredores.append(str(credor['i_credores']))

                            i_credores = ','.join(listCredores)

                            # passo 1.2 - somando a quantidade total de cada item dos credores com tipo 6 que retornaram no passo 1
                            somaQtdItensCredor = banco.consultar(f"""SELECT SUM(qtde_parcial) as totalQtdItem
                                                                from compras.compras.homologa_parcial hp 
                                                                where hp.i_credores in ({i_credores})
                                                                and hp.i_processo = {i['i_processo']}
                                                                and hp.i_ano_proc = {i['i_ano_proc']}
                                                                and hp.i_item = {i['i_item']}
                                                                and hp.i_entidades = {i['i_entidades']};
                                                            """)[0]['totalQtdItem']

                        # passo 2 - coletar a quantidade de cada item do processo
                        qtdItensProcesso = banco.consultar(f"""SELECT ip.qtde_item
                                                                from compras.compras.itens_processo ip 
                                                                where ip.i_ano_proc = {i['i_ano_proc']}
                                                                and ip.i_processo = {i['i_processo']}
                                                                and ip.i_item = {i['i_item']}
                                                                and i_entidades = {i['i_entidades']};
                                                            """)[0]['qtde_item']

                        # passo 3 - caso tenha itens de credores com situação 6, é feito o calculo para saber a diferença para colocar no item do contrato.
                        # caso não tenha credor, a quantidade é o total que retorna no passo 2
                        if somaQtdItensCredor is not None:
                            newQtdItem = qtdItensProcesso - somaQtdItensCredor
                            if newQtdItem < 0:
                                newQtdItem = qtdItensProcesso
                        else:
                            newQtdItem = qtdItensProcesso

                        # passo 4 - coletar o itens da SF para inserir no contrato
                        itenSf = banco.consultar(f"""SELECT hp.preco_unit_adj, hp.preco_total_adj
                                                        from compras.compras.homologa_parcial hp
                                                        where hp.i_ano_proc = {i['i_ano_proc']}
                                                        and hp.i_processo = {i['i_processo']}
                                                        and hp.i_credores = {i['i_credores']} 
                                                        and hp.i_item = {i['i_item']}
                                                        and hp.i_entidades = {i['i_entidades']}
                                                """)[0]

                        # passo 5 - criar o insert do item no contrato
                        precoTotalItem = itenSf['preco_unit_adj'] * newQtdItem
                        comandoInsert += f"""INSERT into compras.compras.contratos_itens (i_contratos, i_entidades, i_item_ctr, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo)
                                                values({i['codContratoAF']}, {i['i_entidades']}, {i['i_item']}, {newQtdItem}, {itenSf['preco_unit_adj']}, {precoTotalItem}, {i['i_ano_proc']}, {i['i_processo']}, {i['i_item']}, {i['i_entidades']});\n
                                            """
                        dadoAlterado.append(f"Inserido o item {i['i_item']} no contrato {i['codContratoAF']} da entidade {i['i_entidades']}")

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_sf_menor_data_inicio_contrato: {e}")
            return

        if item_nao_presente_no_contrato:
            dado = analisa_item_nao_presente_no_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_item_nao_presente_no_contrato(listDados=dado)

    def analisa_corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato(pre_validacao):
        nomeValidacao = "Quantidade solicitada do item, maior que a quantidade de consumo previsa no contrato"

        def analisa_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            if dadosList:
                for i in dadosList:
                    if i['pre_validacao'] == f'{pre_validacao}':
                        dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato(log=False):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            listSequAdj = []

            if log:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""SELECT * FROM (SELECT entidade, -- AS entidade
                                                               ano_proc , -- AS ano_proc
                                                               processo , -- AS processo
                                                               credor , -- AS credor
                                                               item , -- as item
                                                               sum(aut_for.quantidade) AS qtd_sol,
                                                               count(aut_for.i_sequ_adj) as qtdItens,
                                                               list(aut_for.i_sequ_adj) as listI_sequ_adj,
                                                               ci.qtde_ctr,
                                                               codContratoAF 
                                                           FROM ( SELECT distinct
                                                                     hp.i_entidades as entidade,
                                                                     hp.i_ano_proc as ano_proc,
                                                                     hp.i_processo as processo,
                                                                     hp.i_credores as credor,
                                                                     hp.i_sequ_adj as i_sequ_adj,
                                                                     1 as chave_dsk6,
                                                                     hp.i_item as item,
                                                                     isnull(hp.i_ano_desp,0) as chave_dsk8,
                                                                     isnull(hp.i_despesas,0) as chave_dsk9,
                                                                     codContratoAF = isnull((Select min(af.i_contratos) from compras.sequ_autor af
                                                                                             where af.i_entidades = hp.i_entidades
                                                                                               and af.i_entidades_partic  = hp.i_entidades_partic
                                                                                               and af.i_credores  = hp.i_credores
                                                                                               and af.i_processo  = hp.i_processo
                                                                                               and af.i_ano_proc  = hp.i_ano_proc
                                                                                               and af.i_sequ_adj  = hp.i_sequ_adj),0),
                                                                     
                                                                     existeContrato = if codContratoAF > 0 then
                                                                                         codContratoAF
                                                                                      else
                                                                                         isnull((select min(ct.i_contratos) from compras.contratos ct
                                                                                               where ct.i_entidades = hp.i_entidades_partic
                                                                                                 and ct.i_credores  = hp.i_credores
                                                                                                 and ct.i_processo  = hp.i_processo
                                                                                                 and ct.i_ano_proc  = hp.i_ano_proc),0)
                                                                                      endif,					                 
                                                                     data_ass_contrato = if existeContrato > 0 then
                                                                                            (select ct.data_ass from compras.contratos ct
                                                                                                     where ct.i_entidades  = hp.i_entidades_partic
                                                                                                       and ct.i_contratos  = existeContrato)
                                                                                         endif,
                                                                     
                                                                     data_inicio_vigencia = if existeContrato > 0 then
                                                                                               (select ct.data_ini_vig from compras.contratos ct
                                                                                                      where ct.i_entidades  = hp.i_entidades_partic
                                                                                                        and ct.i_contratos  = existeContrato)
                                                                                            endif,
                                                                     
                                                                     data_SF = if isnull(adj.data_af, adj.data_adjudica) < isnull(data_inicio_vigencia, '1900-01-01') then
                                                                                  dateformat(data_inicio_vigencia,'yyyy-mm-dd')
                                                                               else
                                                                                  if isnull(adj.data_af, adj.data_adjudica) < isnull(data_ass_contrato, '1900-01-01') then
                                                                                     dateformat(data_ass_contrato,'yyyy-mm-dd')
                                                                                  else
                                                                                     dateformat(isnull(adj.data_af, adj.data_adjudica),'yyyy-mm-dd')
                                                                                  endif
                                                                               endif,
                                                                     ano_SF = year(data_af),
                                                                     
                                                                     solicitacaoFornec = trim(str(isnull((select first s.i_sequ_aut
                                                                              from compras.sequ_autor s
                                                                             where s.i_entidades = hp.i_entidades_partic
                                                                               and s.i_processo = hp.i_processo
                                                                               and s.i_ano_proc = hp.i_ano_proc
                                                                               and s.i_sequ_adj = hp.i_sequ_adj
                                                                               and s.i_sequ_adj = hp.i_sequ_adj
                                                                               and s.i_credores = hp.i_credores
                                                                             order by s.i_sequ_aut), ''))),
                                                                     
                                                                     itemInclusoContrato = if existeContrato > 0 then
                                                                                          isnull((select first 1 from compras.contratos_itens ci where ci.i_entidades = hp.i_entidades_partic and ci.i_contratos = existeContrato and ci.i_item_ctr = hp.i_item),'')
                                                                                       else
                                                                                          1
                                                                                       endif,					                
                                                                    quantidade = (select cast(sum(isnull(h.qtde_parcial,0)) as numeric(14,3)) from compras.homologa_parcial h where hp.i_entidades_partic = h.i_entidades_partic
                                                                                    and h.i_entidades = hp.i_entidades
                                                                                    and h.i_entidades_partic = hp.i_entidades_partic
                                                                                    and h.i_ano_proc  = hp.i_ano_proc
                                                                                    and h.i_processo = hp.i_processo
                                                                                    and h.i_sequ_adj = hp.i_sequ_adj
                                                                                    and h.i_item = hp.i_item
                                                                                    and h.i_credores = hp.i_credores
                                                                                    and isnull(h.i_ano_desp, 0) = isnull(hp.i_ano_desp, 0)
                                                                                    and isnull(h.i_despesas, 0) = isnull(hp.i_despesas, 0)
                                                                                    ),
                                                                     baseCalculo = isnull(cast(hp.vlr_tabela as numeric(13,2)),0),
                                                                     valorUnitario = isnull(cast(hp.preco_unit_adj as numeric(15,4)),0),
                                                                    valorTotal = (select cast(sum(h.preco_total_adj) as numeric(13,2)) from compras.homologa_parcial h where hp.i_entidades_partic = h.i_entidades_partic
                                                                                    and h.i_entidades = hp.i_entidades
                                                                                    and h.i_entidades_partic = hp.i_entidades_partic
                                                                                    and h.i_ano_proc  = hp.i_ano_proc
                                                                                    and h.i_processo = hp.i_processo
                                                                                    and h.i_sequ_adj = hp.i_sequ_adj
                                                                                    and h.i_item = hp.i_item
                                                                                    and h.i_credores = hp.i_credores
                                                                                    and isnull(h.i_ano_desp, 0) = isnull(hp.i_ano_desp, 0)
                                                                                    and isnull(h.i_despesas, 0) = isnull(hp.i_despesas, 0)
                                                                                    )
                                                           from compras.homologa_parcial hp
                                                                join compras.adjudicacao adj on (adj.i_entidades = hp.i_entidades and adj.i_entidades_partic = hp.i_entidades_partic and adj.i_ano_proc  = hp.i_ano_proc and adj.i_processo = hp.i_processo and adj.i_sequ_adj = hp.i_sequ_adj)
                                                                join compras.processos on (hp.i_entidades = processos.i_entidades and hp.i_ano_proc = processos.i_ano_proc and hp.i_processo = processos.i_processo)
                                                          where solicitacaoFornec > 0
                                                            and year(data_SF) >= 1900 )as aut_for   
                                                                INNER JOIN compras.processos procs ON 
                                                                   procs.i_ano_proc = aut_for.ano_proc AND
                                                                   procs.i_processo = aut_for.processo AND
                                                                   procs.i_entidades = aut_for.entidade
                                                                INNER JOIN compras.contratos_itens ci ON 
                                                                   ci.i_contratos = aut_for.codContratoAF AND
                                                                   ci.i_entidades = aut_for.entidade AND
                                                                   ci.i_item = aut_for.item AND
                                                                   ci.i_ano_proc = aut_for.ano_proc AND
                                                                   ci.i_processo = aut_for.processo
                                           
                                                                WHERE procs.controle_qtd_cred <> 3 -- Ignora processos em que a quantidade é rateada
                                                                GROUP BY entidade, ano_proc, processo, credor, item, codContratoAF, ci.i_item, ci.qtde_ctr
                                                          
                                                          ) AS calc_solicitacoes_procs                  
                                          WHERE qtd_sol > qtde_ctr
                                          ORDER BY ano_proc, processo, item
                                        """)
                for i in dados:
                    listSequAdj = [i['listI_sequ_adj']]

                    listSequAdj = ','.join(listSequAdj)
                    listSequAdj = listSequAdj.split(',')
                    for sequAdj in listSequAdj:
                        newQtd = math.floor(i['qtde_ctr'] / i['qtdItens'])

                        comandoUpdate += f"UPDATE compras.compras.homologa_parcial set qtde_parcial = {newQtd} , preco_total_adj = {newQtd}*preco_unit_adj where i_ano_proc = {i['ano_proc']} and i_processo = {i['processo']} and i_credores = {i['credor']} and i_item = {i['item']} and i_sequ_adj = {sequAdj};\n"
                        dadoAlterado.append(f"Alterado a quantidade do item {i['item']} para {newQtd} do contrato {i['codContratoAF']} da entidade {i['entidade']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                if log:
                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato: {e}")
            return

        if qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato:
            dado = analisa_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato(log=True)
            elif corrigirErros:
                corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato()

    if dadosList:
        analisa_corrige_item_nao_presente_no_contrato(pre_validacao='item_nao_presente_no_contrato')

    analisa_corrige_qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato(pre_validacao='qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato')
