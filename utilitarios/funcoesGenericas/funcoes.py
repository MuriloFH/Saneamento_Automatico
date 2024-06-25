from utilitarios.conexao.conectaOdbc import Conecta
from validate_docbr import CPF, CNPJ, RENAVAM, CNH, PIS
from datetime import datetime, timedelta
import math
import random
import string
import re
import sys
import colorama


def geraCfp():
    return CPF().generate()


def geraPis():
    return PIS().generate()


def validaPis(pis):
    return PIS().validate(str(pis))


def validaCPF(cpf):
    return CPF().validate(str(cpf))


def geraCnpj():
    return CNPJ().generate()


def validaCnpj(cpf):
    return CNPJ().validate(str(cpf))


def geraModeloFipe():
    newModeloFipe = ""
    for i in range(12):
        newModeloFipe += str(random.randint(0, 9))

    return newModeloFipe


def geraInscricaoEstadual():
    newInscricaoEstadual = ""
    for i in range(12):
        newInscricaoEstadual += str(random.randint(0, 9))

    return newInscricaoEstadual


def generateInscricaoMunicipal():
    newInscricaoMunicipal = ""
    for i in range(11):
        newInscricaoMunicipal += str(random.randint(0, 9))

    return newInscricaoMunicipal


def geraCnh():
    return CNH().generate()


def geraRenavam():
    return RENAVAM().generate()


def geraChassi():
    letters_and_digits = string.ascii_letters + string.digits
    return ''.join(random.choice(letters_and_digits) for i in range(20))


def geraNumeroPatrimonio():
    newPatrimonio = ""
    for i in range(random.randint(1, 19)):
        newPatrimonio += str(random.randint(0, 9))

    return newPatrimonio


def remove_caracteres_especiais(string):
    return re.sub(r'[^0-9]+', '', string)


def find_emails(string):
    """
    :param string: emails
    :return: Retorna o primeiro e-mail da lista de e-mails criada
    """
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.findall(pattern, string)[0]


def corrige_capacidade_volumetrica_nula(listVeiculos, banco):
    newCapacidade = 55.0
    comandoUpdate = f"UPDATE bethadba.veiculos set limite = {newCapacidade} where i_veiculos in ({listVeiculos});\n"

    return banco.executar(comando=banco.triggerOff(comandoUpdate))


def geraCodSiafi():
    newCodSiafi = ""
    for i in range(9):
        newCodSiafi += str(random.randint(0, 9))
    return newCodSiafi


def geraPlacaBem(idBem, dtAquisicao):
    aquisicao = str(dtAquisicao).replace('-', "")
    placa = f"{f'{idBem}{aquisicao}'.zfill(12)}"
    return placa[:12]


def geraNumeroLicitacao(numLicitacaoAtual):
    while True:
        newNum = random.randint(0, 100)
        if newNum != numLicitacaoAtual:
            return newNum


def getICidade(odbc, entidade, nomeCidade):
    banco = Conecta(odbc=odbc)
    if "'" in nomeCidade:
        nome = nomeCidade.replace("'", "")
    else:
        nome = nomeCidade

    busca = banco.consultar(f"""SELECT i_cidades, nome from compras.compras.cidades c where c.nome = '{nome}'""")
    if len(busca) > 0:
        iCidade = busca[0]['i_cidades']
    else:
        iCidade = banco.consultar(f"""select i_cidades from compras.parametros where i_entidades = {entidade}""")[0]['i_cidades']

    return iCidade


def getDadosItenContrato(i_chave_dsk1, i_chave_dsk2, banco, idCredor=None):
    sql = f"""SELECT c.i_contratos,
                c.i_entidades,
                ip.i_item,
                ip.i_material,
                isnull((if p2.processo_multientidade = 'S' then 
                         (select processos_entidades_partic_itens.qtd_estimada
                            from compras.processos_entidades_partic_itens
                           where processos_entidades_partic_itens.i_ano_proc  = p.i_ano_proc
                             and processos_entidades_partic_itens.i_entidades = p.i_entidades
                             and processos_entidades_partic_itens.i_processo  = p.i_processo
                             and processos_entidades_partic_itens.i_entidades = p.i_entidades
                             and processos_entidades_partic_itens.i_item      = p.i_item
                             and processos_entidades_partic_itens.i_entidades_partic = p2.i_entidades) 
                      else 
                         p.qtde_cotada
                      endif),0) as qtde_ctr,
                (if p2.processo_multientidade = 'S' then 
                     (select (processos_entidades_partic_itens.qtd_estimada * p.preco_unit_part)
                        from compras.processos_entidades_partic_itens
                       where processos_entidades_partic_itens.i_ano_proc  = p.i_ano_proc
                         and processos_entidades_partic_itens.i_entidades = p.i_entidades
                         and processos_entidades_partic_itens.i_processo  = p.i_processo
                         and processos_entidades_partic_itens.i_entidades = p.i_entidades
                         and processos_entidades_partic_itens.i_item      = p.i_item
                         and processos_entidades_partic_itens.i_entidades_partic = p2.i_entidades) 
                  else 
                     p.preco_total
                 endif) as preco_total_ctr,
                isnull(p.preco_unit_part, (select cia.preco_unit_novo
                                            from compras.contratos_itens_apostila as cia 
                                            where cia.i_contratos = c.i_contratos 
                                            and cia.i_entidades = c.i_entidades 
                                            and cia.i_item_apostila = ip.i_item 
                                            and cia.i_sequ_hist = (select first i_sequ_hist
                                                                    from compras.contratos_historico as ch
                                                                    where exists(select 1 
                                                                                    from compras.contratos_itens_apostila cia2 
                                                                                    where cia2.i_entidades = ch.i_entidades 
                                                                                      and cia2.i_sequ_hist = ch.i_sequ_hist 
                                                                                      and cia2.i_contratos = ch.i_contratos 
                                                                                      and cia2.i_item_apostila = ip.i_item)
                                                                    and ch.i_contratos = c.i_contratos 
                                                                    and ch.i_entidades = c.i_entidades 
                                                                    and ch.tipo_historico in( 6,7,13) 
                                                                    order by ch.data_historico desc,ch.i_sequ_hist desc))
                 ) as preco_unit_ctr,
                 c.i_ano_proc,
                 c.i_processo, 
                 ip.i_item,
                 c.i_entidades,
                 p.percent_bdi_tce,
                 p.percent_encargo_tce,
                 p.situacao,
                 c.i_credores
                from compras.compras.contratos c
                join compras.compras.processos p2 on (p2.i_ano_proc = c.i_ano_proc and p2.i_processo = c.i_processo and p2.i_entidades = c.i_entidades)
                join compras.compras.participantes p on (p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo and p.i_credores = c.i_credores)
                join compras.compras.itens_processo ip on (ip.i_ano_proc = p.i_ano_proc and ip.i_processo = p.i_processo and ip.i_entidades = p.i_entidades and ip.i_item = p.i_item)
                where c.i_contratos = {i_chave_dsk2} and c.i_entidades = {i_chave_dsk1} and p.situacao  = 2
                order by ip.i_item
                                """
    if idCredor is None:
        dado = banco.consultar(sql)
    else:
        sql = sql.replace(' and p.situacao  = 2', f' and c.i_credores = {idCredor}')
        dado = banco.consultar(sql)
    return dado


def adicionaDiasData(data, qtdDias):
    newData = data
    newData += timedelta(days=qtdDias)
    return newData


def getDadosSolicitacoesFornecimentoValidacao(conexaoBanco, validacao: str):
    sql = f"""SELECT * 
                from(SELECT DISTINCT
                         hp.i_entidades_partic as i_entidades_partic,  
                         hp.i_ano_proc as i_ano_proc, 
                         hp.i_processo as i_processo,  
                         hp.i_credores as i_credores,  
                         hp.i_sequ_adj as i_sequ_adj,
                         adj.i_entidades as i_entidades,  
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
                                                      
                         sequenciaSF = (select first s.i_sequ_aut
                                  from compras.sequ_autor s
                                 where s.i_entidades = sequ_autor.i_entidades
                                   and processos.i_processo  = s.i_processo
                                   and processos.i_ano_proc  = s.i_ano_proc
                                   and hp.i_sequ_adj = s.i_sequ_adj
                                   and adj.i_sequ_adj = s.i_sequ_adj
                                   and hp.i_credores = s.i_credores
                                 order by s.i_sequ_aut),
                         data_SF = dateformat(isnull(adj.data_af, adj.data_adjudica, '1900-01-01'), 'yyyy-mm-dd'),
                         data_inicio_vigencia = if existeContrato > 0 then
                                                   (select ct.data_ini_vig from compras.contratos ct
                                                          where ct.i_entidades  = hp.i_entidades_partic 
                                                            and ct.i_contratos  = existeContrato)
                                                endif,
                        data_fim_vigencia = if existeContrato > 0 then
                                                   (select ct.data_vcto from compras.contratos ct
                                                          where ct.i_entidades  = hp.i_entidades_partic 
                                                            and ct.i_contratos  = existeContrato)
                                                endif
                    from compras.homologa_parcial hp,
                         compras.adjudicacao adj,
                         compras.processos,
                         compras.sequ_autor
                   where adj.i_entidades = hp.i_entidades
                     and adj.i_entidades_partic = hp.i_entidades_partic
                     and adj.i_ano_proc  = hp.i_ano_proc
                     and adj.i_processo = hp.i_processo
                     and adj.i_sequ_adj = hp.i_sequ_adj
                     and processos.i_entidades = hp.i_entidades
                     and processos.i_ano_proc  = hp.i_ano_proc
                     and processos.i_processo  = hp.i_processo
                     and processos.i_processo  = sequ_autor.i_processo
                     and processos.i_ano_proc  = sequ_autor.i_ano_proc
                     and hp.i_sequ_adj = sequ_autor.i_sequ_adj
                     and adj.i_sequ_adj = sequ_autor.i_sequ_adj
                     and hp.i_credores = sequ_autor.i_credores
                ) as autorizacoes
                where autorizacoes.existeContrato > 0
                and $validacao$
        """
    if validacao == 'data_sf_menor_data_inicio_contrato':
        sql = sql.replace('$validacao$', 'autorizacoes.data_SF < data_inicio_vigencia')
    elif validacao == 'data_sf_menor_data_fim_contrato':
        sql = sql.replace('$validacao$', 'autorizacoes.data_SF > data_fim_vigencia')
    else:
        sys.exit("O parametro 'validação' da função 'getDadosSolicitacoesFornecimentoValidacao' pode receber apenas 'data_sf_menor_data_inicio_contrato' ou 'data_sf_menor_data_fim_contrato'")

    busca = conexaoBanco.consultar(sql)
    return busca


def getDadosDataHomologacaoProcessoMaiorDataSf(conexaoBanco):
    sql = f"""SELECT * 
                from(SELECT DISTINCT 'solicitacoes-fornecimento' as tipo_registro,
                         hp.i_entidades_partic as chave_dsk1,  
                         hp.i_ano_proc as chave_dsk2, 
                         hp.i_processo as chave_dsk3,  
                         hp.i_credores as chave_dsk4,  
                         hp.i_sequ_adj as chave_dsk5,  
                         1 as chave_dsk6,
                         processos.i_ano_proc as anoProcessamento, 
                         sequ_autor.i_sequ_adj as i_item_adit,
                         305 as sistema,
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
                                                     
                         contratacao = if existeContrato > 0 then
                                          ISNULL(bethadba.dbf_get_id_gerado(sistema,'contratacoes', chave_dsk1, existeContrato, 0, 0),'') 
                                       else 
                                          ISNULL(bethadba.dbf_get_id_gerado(sistema,'contratacoes', chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4),'') 
                                       endif,
                         catalogo = isnull(adj.numero_catalogo,0),
                                                                                          
                         itemTabelaPreco = if isnull(adj.numero_catalogo,0) > 0 then 
                                              if existeContrato > 0 then
                                                 isnull(bethadba.dbf_get_id_gerado(sistema,'contratacoes-itens', chave_dsk1, adj.numero_catalogo, existeContrato, 0, 0, chave_dsk1),'')
                                              else
                                                 isnull(bethadba.dbf_get_id_gerado(sistema,'contratacoes-itens', chave_dsk1, adj.numero_catalogo, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk1),'')
                                              endif
                                           else 
                                              ''
                                           endif,
        
                         codCCustoAF = isnull((Select min(af.i_ccusto) from compras.sequ_autor af
                                                 where af.i_entidades = hp.i_entidades 
                                                   and af.i_entidades_partic = hp.i_entidades_partic 
                                                   and af.i_credores  = hp.i_credores 
                                                   and af.i_processo  = hp.i_processo 
                                                   and af.i_ano_proc  = hp.i_ano_proc
                                                   and af.i_sequ_adj  = hp.i_sequ_adj),0),
                                                   
                          anoCCustoAF = isnull((Select first(af.i_ano) from compras.sequ_autor af
                                                 where af.i_entidades = hp.i_entidades 
                                                   and af.i_entidades_partic = hp.i_entidades_partic 
                                                   and af.i_credores  = hp.i_credores 
                                                   and af.i_processo  = hp.i_processo 
                                                   and af.i_ano_proc  = hp.i_ano_proc
                                                   and af.i_sequ_adj  = hp.i_sequ_adj
                                                   and af.i_ccusto = codCCustoAF order by 1 ),0),
                                                                                                                  
                         ccusto_menor = if codCCustoAF > 0 then
                                           codCCustoAF
                                        else
                                           isnull((Select min(hp1.i_ccusto) from compras.homologa_parcial hp1
                                                    where hp1.i_entidades = hp.i_entidades
                                                      and hp1.i_entidades_partic = hp.i_entidades_partic
                                                      and hp1.i_ano_proc = hp.i_ano_proc 
                                                      and hp1.i_processo = hp.i_processo 
                                                      and hp1.i_credores = hp.i_credores 
                                                      and hp1.i_sequ_adj = hp.i_sequ_adj),0)
                                        endif,
        
                         anoCCustoMenor = if codCCustoAF > 0 then
                                           anoCCustoAF
                                        else
                                           isnull((Select first(hp1.i_ano) from compras.homologa_parcial hp1
                                                    where hp1.i_entidades = hp.i_entidades
                                                      and hp1.i_entidades_partic = hp.i_entidades_partic
                                                      and hp1.i_ano_proc = hp.i_ano_proc 
                                                      and hp1.i_processo = hp.i_processo 
                                                      and hp1.i_credores = hp.i_credores 
                                                      and hp1.i_sequ_adj = hp.i_sequ_adj
                                                 order by 1
                                                 ),0)
                                        endif,
                                        
                         ccusto_padrao = if ccusto_menor > 0 then
                                            ccusto_menor
                                         else
                                            if not processos.i_ccusto is null then
                                               processos.i_ccusto
                                            else 
                                               Isnull((Select min(cc1.i_ccusto) from compras.ccustos cc1
                                                       where cc1.i_ano = hp.i_ano_proc and
                                                             cc1.i_entidades = hp.i_entidades_partic),0)
                                            endif 
                                         endif,   
                         
                          anoCcustoPadrao = if ccusto_menor > 0 then
                                            anoCCustoMenor
                                         else
                                            if not processos.i_ccusto is null then
                                               processos.i_ano
                                            else 
                                               Isnull((Select first(cc1.i_ano) from compras.ccustos cc1
                                                       where cc1.i_ano = hp.i_ano_proc and
                                                             cc1.i_entidades = hp.i_entidades_partic
                                                          and cc1.i_ccusto = ccusto_padrao order by 1),0)
                                            endif 
                                         endif, 
        
                        entid_partic = 0, //SOMENTE PARA ADITIVOS                                                                   
                                          
                         mask = (Select right('00'+string(cc.i_orgaos),2)+right('000'+string(cc.i_unidades),3)+ right('00000'+string(cc.i_ccusto), 5)   
                                    from compras.ccustos cc 
                                   where cc.i_entidades = hp.i_entidades_partic and 
                                         cc.i_ccusto = ccusto_padrao and 
                                         cc.i_ano = anoCcustoPadrao),
        
                         organograma  = isnull(bethadba.dbf_get_id_gerado(sistema,'organogramas',chave_dsk1, anoCcustoPadrao, mask),''), 
                         
                         localEntrega = isnull(bethadba.dbf_get_id_gerado(sistema,'locais-entrega',chave_dsk1,processos.i_local),''),  
                         prazoEntrega = if (processos.prazo_entrega is null) then 
                                           isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','IMEDIATA',chave_dsk1),'')
                                        else
                                           isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega',trim(upper(processos.prazo_entrega)),chave_dsk1),isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','IMEDIATA',chave_dsk1), isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','Imediata',chave_dsk1),'')))
                                        endif,
                       
                         nomeSolicitante = 'Migração dos dados',
                         sequenciaSF = (select first s.i_sequ_aut
                                  from compras.sequ_autor s
                                 where s.i_entidades = sequ_autor.i_entidades
                                   and processos.i_processo  = s.i_processo
                                   and processos.i_ano_proc  = s.i_ano_proc
                                   and hp.i_sequ_adj = s.i_sequ_adj
                                   and adj.i_sequ_adj = s.i_sequ_adj
                                   and hp.i_credores = s.i_credores
                                 order by s.i_sequ_aut),
                         
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
                                                
                         data_fim_vigencia = if existeContrato > 0 then
                                                   (select ct.data_vcto from compras.contratos ct
                                                          where ct.i_entidades  = hp.i_entidades_partic 
                                                            and ct.i_contratos  = existeContrato)
                                                endif,
                                                
                         existeAditivo = isnull((select first c.i_contratos from compras.contratos c 
                                                 where c.i_entidades = hp.i_entidades_partic
                                                 and c.i_contratos_sup_compras = existeContrato), 0),
                                                                                                    
                         data_SF = dateformat(isnull(adj.data_af, adj.data_adjudica, '1900-01-01'), 'yyyy-mm-dd') ,                             
                         
                         observacao = string('Solicitação de Fornecimento gerada pela migração. Processo: ' +string(adj.i_processo) +'/' +string(adj.i_ano_proc) +'  -  Adjudicação: ' +string(adj.i_sequ_adj)), 
                         isnull(bethadba.dbf_get_id_gerado(sistema, tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5, chave_dsk6),'') as id_gerado,            
                         
                         fornecedor = isnull(bethadba.dbf_get_id_gerado(sistema,'fornecedores',chave_dsk1,hp.i_credores),''),
                         processos.data_homolog as data_homologacao
        
                    from compras.homologa_parcial hp,
                         compras.adjudicacao adj,
                         compras.processos,
                         compras.sequ_autor
        
                   where adj.i_entidades = hp.i_entidades
                     and adj.i_entidades_partic = hp.i_entidades_partic
                     and adj.i_ano_proc  = hp.i_ano_proc
                     and adj.i_processo = hp.i_processo
                     and adj.i_sequ_adj = hp.i_sequ_adj
                     and processos.i_entidades = hp.i_entidades
                     and processos.i_ano_proc  = hp.i_ano_proc
                     and processos.i_processo  = hp.i_processo
                     and processos.i_processo  = sequ_autor.i_processo
                     and processos.i_ano_proc  = sequ_autor.i_ano_proc
                     and hp.i_sequ_adj = sequ_autor.i_sequ_adj
                     and adj.i_sequ_adj = sequ_autor.i_sequ_adj
                     and hp.i_credores = sequ_autor.i_credores
        
        UNION
        
        SELECT DISTINCT 'solicitacoes-fornecimento' as tipo_registro,
                         adit.i_entidades as chave_dsk1, 
                         adit.i_contratos as chave_dsk2, 
                         sequ_autor.i_processo as chave_dsk3,  // NÃO UTILIZADO NO ADITAMENTO
                         sequ_autor.i_entidades as chave_dsk4,  // NÃO UTILIZADO NO ADITAMENTO
                         adit.i_sequ_adit chave_dsk5, 
                         2 as chave_dsk6,   //1=homologa_parcial  2=Aditivos 
                         processos.i_ano_proc as anoProcessamento, 
                         adit.i_item_adit as i_item_adit,
                         305 as sistema,
                         codContratoAF  = ctr_adit.i_contratos_sup_compras,
                         existeContrato = ctr_adit.i_contratos_sup_compras,    // Esta coluna refere-se ao código do contrato principal
                                                     
                         contratacao = ISNULL(bethadba.dbf_get_id_gerado(sistema,'contratacoes', chave_dsk1, existeContrato, 0, 0),''),
                         catalogo = 0,                    
                         itemTabelaPreco = '',
        
                         codCCustoAF = isnull((Select min(af.i_ccusto) from compras.sequ_autor af
                                              where af.i_entidades = adit.i_entidades 
                                                and af.i_credores  = ctr_adit.i_credores 
                                                and af.i_processo  = ctr_adit.i_processo 
                                                and af.i_ano_proc  = ctr_adit.i_ano_proc
                                                and af.i_sequ_adit  = adit.i_sequ_adit),0),
                         
                        anoCCustoAF = isnull((Select first(af.i_ano) from compras.sequ_autor af
                                                 where af.i_entidades = adit.i_entidades 
                                                   and af.i_credores  = ctr_adit.i_credores 
                                                   and af.i_processo  = ctr_adit.i_processo
                                                   and af.i_ano_proc  =ctr_adit.i_ano_proc
                                                   and af.i_sequ_adit  = adit.i_sequ_adit
                                                   and af.i_ccusto = codCCustoAF order by 1 ),0),
                                                                                                 
                         ccusto_menor = if codCCustoAF > 0 then
                                           codCCustoAF
                                        else
                                           0
                                        endif, 
                     
                        anoCCustoMenor = if codCCustoAF > 0 then
                                           anoCCustoAF
                                        else
                                           0
                                        endif,
                                       
        
                         ccusto_padrao = if ccusto_menor > 0 then
                                            ccusto_menor
                                         else
                                            Isnull((Select min(cc1.i_ccusto) from compras.ccustos cc1
                                                     where cc1.i_ano = ctr_adit.i_ano_proc and
                                                           cc1.i_entidades = adit.i_entidades),0)
                                         endif,  
        
                          anoCcustoPadrao = if ccusto_menor > 0 then
                                                anoCCustoMenor
                                            else
                                                if not processos.i_ccusto is null then
                                                    processos.i_ano
                                                else 
                                                    Isnull((Select first(cc1.i_ano) from compras.ccustos cc1
                                                             where cc1.i_ano = ctr_adit.i_ano_proc and
                                                                   cc1.i_entidades = adit.i_entidades and
                                                                   cc1.i_ccusto = ccusto_padrao order by 1)
                                                        ,0)
                                            endif 
                                         endif,                
        
                         entid_partic = (Select first i_entidades_partic from compras.sequ_autor af
                                              where af.i_entidades = adit.i_entidades 
                                                and af.i_credores  = ctr_adit.i_credores 
                                                and af.i_processo  = ctr_adit.i_processo 
                                                and af.i_ano_proc  = ctr_adit.i_ano_proc
                                                and af.i_sequ_adit  = adit.i_sequ_adit and i_ccusto is not null order by i_ccusto),                                  
         
                         mask = (Select right('00'+string(cc.i_orgaos),2)+right('000'+string(cc.i_unidades),3)+ right('00000'+string(cc.i_ccusto), 5)   
                                    from compras.ccustos cc 
                                   where cc.i_entidades = isnull(entid_partic,adit.i_entidades) and 
                                         cc.i_ccusto = ccusto_padrao and 
                                         cc.i_ano = anoCcustoPadrao),
                         organograma  = isnull(bethadba.dbf_get_id_gerado(sistema,'organogramas',isnull(entid_partic,chave_dsk1), anoCcustoPadrao , mask),''), 
                           
                         localEntrega = isnull(bethadba.dbf_get_id_gerado(sistema,'locais-entrega',processos.i_entidades,processos.i_local),''),  
                         prazoEntrega = if (processos.prazo_entrega is null) then 
                                           isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','IMEDIATA',chave_dsk1),'')
                                        else
                                           isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega',trim(upper(processos.prazo_entrega)),chave_dsk1),isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','IMEDIATA',chave_dsk1), isnull(bethadba.dbf_get_id_gerado(sistema,'prazos-entrega','Imediata',chave_dsk1),'')))
                                        endif,
                       
                         nomeSolicitante = 'Migração dos dados',
                         sequenciaSF = (select first s.i_sequ_aut
                                          from compras.sequ_autor s
                                         where s.i_entidades = ctr_adit.i_entidades
                                            and s.i_contratos = ctr_adit.i_contratos
                                            and s.i_sequ_adit = adit.i_sequ_adit
                                           order by 1),
        
                         data_ass_contrato = dateformat(adit.data_adit, 'yyyy-mm-dd'), 
                         data_inicio_vigencia = dateformat(adit.data_adit, 'yyyy-mm-dd'),
                         data_fim_vigencia = isnull((select dateformat(ct.data_vcto, 'yyyy-mm-dd') from compras.contratos ct
                                                          where ct.i_entidades  = ctr_adit.i_entidades 
                                                            and ct.i_contratos  = ctr_adit.i_contratos), '1900-01-01'), 
                         existeAditivo = 0,
                         data_SF = date(adit.data_adit),
                         observacao = string('Solicitação de Fornecimento gerada pela migração. Contrato: ' +string(ctr_adit.i_contratos_sup_compras) +'  -  Aditivo: ' +string(adit.i_sequ_adit)), 
                         isnull(bethadba.dbf_get_id_gerado(sistema, tipo_registro, chave_dsk1, chave_dsk2, chave_dsk3, chave_dsk4, chave_dsk5, chave_dsk6),'') as id_gerado,            
                         fornecedor = isnull(bethadba.dbf_get_id_gerado(sistema,'fornecedores',chave_dsk1,ctr_adit.i_credores),''),
                         processos.data_homolog as data_homologacao
        
                    from compras.aditamento adit,
                         compras.contratos ctr_adit,
                         compras.processos,
                         compras.sequ_autor
        
                   where ctr_adit.i_entidades = adit.i_entidades
                     and ctr_adit.i_contratos = adit.i_contratos     
                     and processos.i_entidades = ctr_adit.i_entidades_processo
                     and processos.i_ano_proc  = ctr_adit.i_ano_proc
                     and processos.i_processo  = ctr_adit.i_processo
                     and ctr_adit.natureza > 1
                     and sequ_autor.i_entidades = ctr_adit.i_entidades
                     and sequ_autor.i_contratos = ctr_adit.i_contratos
                     and adit.i_sequ_adit = sequ_autor.i_sequ_adit
                     order by 2,3,4,5,6,7
                ) as autorizacoes
                where data_homologacao > data_SF
            """

    busca = conexaoBanco.consultar(sql)
    return busca


def geraDataSfMaiorQueDataHomologacao(inicioVigencia, fimVigencia, dataHomologacao):
    qtdDatasGeradas = 0
    newDataAdit = dataHomologacao
    qtdDias = fimVigencia - inicioVigencia
    while True:
        # aqui verifica se a newDataAdit que recebeu a mesma data de homologação está entre a data inicio e fim do contrato
        if inicioVigencia <= newDataAdit <= fimVigencia:
            break
        # caso não esteja, a newDataAdit vai receber um calculo com base na diferença de dias do inicio e fim do contrato
        else:
            # feito o while com a condição 'qtdDatasGeradas < 5' pois caso seja criado 5 datas e elas não estejam dentro dos periodos corretos, será exibido uma mensagem de erro no log
            while qtdDatasGeradas < 5:
                randDias = random.randint(1, qtdDias.days)
                newDataAdit = adicionaDiasData(data=inicioVigencia, qtdDias=randDias)
                if (inicioVigencia <= newDataAdit <= fimVigencia) and (newDataAdit >= dataHomologacao):
                    break
                else:
                    qtdDatasGeradas += 1
                # caso seja gerados 5 datas e nenhuma atende os requisitos, é retornado uma mensagem de erro.
                if qtdDatasGeradas == 5:
                    print(colorama.Fore.RED, f'Não ajustado a data da SF. Verifique a data de homologação do processo.', colorama.Fore.RESET)
        break

    # aqui verifica se a qtdDatasGeradas for diferente de 5, caso seja, foi gerado uma data válida para o update.
    if qtdDatasGeradas != 5:
        return newDataAdit
    else:
        return ""


def getItensContratos(banco, i_contratos, i_entidades):
    dados = banco.consultar(f"""SELECT *
                                from compras.compras.contratos_itens ci 
                                where ci.i_contratos = {i_contratos}
                                and ci.i_entidades = {i_entidades}
                                order by ci.i_item_ctr
                            """)
    return dados


def validaDtInicioRecebimentoMaiorDtFimRecebimento(banco):
    comandoUpdate = ""
    busca = banco.consultar(f"""SELECT isnull(dateformat(p.data_entrega + p.hora_entrega, 'yyyy-mm-dd hh:mm:ss'), '') as dataInicioRecebimentoEnvelope,
                                isnull(dateformat(p.data_recebimento + p.hora_recebimento, 'yyyy-mm-dd hh:mm:ss'), '') as dataFinalRecebimentoEnvelope,
                                p.i_ano_proc, i_processo, modalidade, i_entidades, data_entrega, hora_entrega, hora_recebimento, data_processo
                                from compras.compras.processos p 
                                where dataInicioRecebimentoEnvelope > dataFinalRecebimentoEnvelope
                                and dataFinalRecebimentoEnvelope != ''
                                and dataInicioRecebimentoEnvelope != ''
                            """)
    if len(busca) > 0:
        for row in busca:
            newDataRecebimento = row['data_processo']
            newHoraRecebimento = row['hora_recebimento']

            comandoUpdate += f"""UPDATE compras.compras.processos set data_entrega = '{newDataRecebimento}' , hora_entrega = '{newHoraRecebimento}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
            banco.executar(banco.triggerOff(comandoUpdate))
    else:
        return False


def validaItemNaoPresenteContrato(contrato, iten, entidade, banco):
    achou = banco.consultar(f"""SELECT first i_item_ctr 
                        from compras.compras.contratos_itens ci 
                        where ci.i_contratos = {contrato} and ci.i_entidades = {entidade} and ci.i_item_ctr = {iten};
                    """)
    if len(achou) > 0:
        return False
    else:
        return True


def validaQtdItemSfMaiorQtdConsumoContrato(banco):
    comandoUpdate = ""
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

    banco.executar(banco.triggerOff(comandoUpdate))


def limpaItensContratos(i_contratos, i_entidades, banco):
    a = banco.executar(banco.triggerOff(f"""DELETE from compras.contratos_itens WHERE i_contratos = {i_contratos} and i_entidades = {i_entidades};"""))
    if a:
        return True
    else:
        return False


def generateRG():
    newRG = ""
    for i in range(9):
        newRG += str(random.randint(0, 9))

    return newRG


def reordenaSequencialAfastamentos(entidade, funcionario, banco):
    comandoUpdate = ""
    afastamentos = banco.consultar(f"""
                        SELECT a.i_entidades, a.i_funcionarios, a.dt_afastamento
                        from bethadba.afastamentos a 
                        where a.i_entidades = {entidade} and i_funcionarios = {funcionario} and sequencial is not null
                        order by a.dt_afastamento asc
                    """)

    if len(afastamentos) > 0:
        for k, v in enumerate(afastamentos):
            comandoUpdate += f"""UPDATE bethadba.afastamentos set sequencial = {k} where i_entidades = {v['i_entidades']} and i_funcionarios = {v['i_funcionarios']} and dt_afastamento = {v['dt_afastamento']};\n"""

        banco.executar(banco.triggerOff(comandoUpdate, folha=True))
    else:
        return False


def replaceNoneParaNull(lista):
    newListDict = []
    for d in lista:
        newItem = {k: v if v is not None else "null" for k, v in d.items()}
        newListDict.append(newItem)
    return newListDict


def removeDiasData(data, qtdDias):
    newData = data
    newData -= timedelta(days=qtdDias)
    return newData


def insertAgenteIntegracao(banco):
    comandoUpdate = ""
    try:
        busca = banco.consultar(f"""
            select 
                e.i_entidades, 
                e.i_funcionarios,
                hf.dt_alteracoes, 
                hf.i_agente_integracao_estagio,
                (select FIRST  i_agente_integracao_estagio from bethadba.hist_funcionarios where i_agente_integracao_estagio is not null group by i_agente_integracao_estagio order by COUNT(*) desc) as novo_agente_integr
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
                print(colorama.Fore.RED, "Não localizado nenhum agente de integração localizado na função 'insertAgenteIntegracao', favor analisar os casos manualmente.", colorama.Fore.RESET)
                return None

            for row in busca:
                comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set i_agente_integracao_estagio = {row['novo_agente_integr']} where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

        banco.executar(comando=banco.triggerOff(comandoUpdate, folha=True))

    except Exception as e:
        print(f"Erro na função insertAgenteIntegracao: {e}")


def insertResponsavel(banco):
    comandoUpdate = ""
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
                comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set i_supervisor_estagio = {responsavel} where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

        banco.executar(comando=banco.triggerOff(comandoUpdate, folha=True))

    except Exception as e:
        print(f"Erro na função insertResponsavel: {e}")

