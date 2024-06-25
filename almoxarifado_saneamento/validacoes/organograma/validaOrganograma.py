from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd


def organograma(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   mascara_incorreta_nivel_zerado=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_mascara_incorreta_nivel_zerado(pre_validacao):
        nomeValidacao = "Centro de Custo possui sua máscara incorreta, ou a máscara do centro de custo é do 3º nível, com o 2º nível zerado"

        def analisa_mascara_incorreta_nivel_zerado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_mascara_incorreta_nivel_zerado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select  i_entidades,
                                                i_ano_ccusto,
                                               (SELECT if locate(maskccusto,'-') > 1 then '-' else '.' endif
                                                  FROM bethadba.parametros) as separador,
                                               (SELECT length(left(maskccusto,locate(maskccusto,'-')-1))
                                                  FROM bethadba.parametros)as qtd_maskOrgao,
                                               (SELECT length(left(substr(maskccusto,locate(maskccusto,separador)+1),locate(substr(maskccusto,locate(maskccusto,separador)+1),separador)-1))
                                                  FROM bethadba.parametros)as qtd_maskUnidade,
                                               (SELECT length(substr(maskccusto,locate(maskccusto,separador) + (locate(substr(maskccusto,locate(maskccusto,separador)+1),separador)+1)))
                                                  FROM bethadba.parametros)as qtd_mask,
                                               right(repeat('0',qtd_maskOrgao)||trim(ccustos.nivel1),qtd_maskOrgao) as mascara_orgao,
                                               (select cast(max(cc.nivel2) as int) from bethadba.ccustos cc where cc.nivel1 = mascara_orgao) as maximo_unidade,
                                               right(repeat('0',qtd_maskUnidade)||trim(ccustos.nivel2),qtd_maskUnidade) as mascara_unidade,
                                               right(repeat('0',qtd_mask)||trim(ccustos.nivel3),qtd_mask) as mascara_ccusto,
                                               if( (right(repeat('0',qtd_maskOrgao)||trim(ccustos.nivel1),qtd_maskOrgao) = repeat('0',qtd_maskOrgao)) 
                                                   or (right(repeat('0',qtd_mask)||trim(ccustos.nivel3),qtd_mask) <> repeat('0',qtd_mask) 
                                                   and (right(repeat('0',qtd_maskUnidade)||trim(ccustos.nivel2),qtd_maskUnidade) = repeat('0',qtd_maskUnidade)))
                                               ) then 1 else 0 endif as nivelIncorreto,
                                               if(trim(ccustos.nivel1) = '' or trim(ccustos.nivel2) = '' or trim(ccustos.nivel3) = '')  then 1 else 0 endif nivelVazio,
                                               ccustos.i_ccusto as codCcusto,
                                               ccustos.i_ano_ccusto as anoCcusto,
                                               (select first maskccusto from bethadba.parametros) as mascaraCcusto,
                                               right(repeat('0',2)||trim(nivel1),2) || right(repeat('0',3)||trim(nivel2),3) || right(repeat('0',5)||trim(nivel3),5) as numeroCC
                                          from bethadba.ccustos 
                                         where not exists(select 1 from bethadba.config_ccustos)
                                           and (nivelIncorreto <> 0 or nivelVazio <> 0)
                                        UNION 
                                        select  i_entidades,
                                                i_ano_ccusto,
                                               (SELECT if locate(maskccusto,'-') > 1 then '-' else '.' endif
                                                  FROM bethadba.parametros) as separador,
                                               (SELECT length(left(maskccusto,locate(maskccusto,separador)-1))
                                                  FROM bethadba.parametros)as qtd_maskOrgao,
                                               (SELECT length(left(substr(maskccusto,locate(maskccusto,separador)+1),locate(substr(maskccusto,locate(maskccusto,separador)+1),separador)-1))
                                                  FROM bethadba.parametros)as qtd_maskUnidade,
                                               (SELECT length(substr(maskccusto,locate(maskccusto,separador) + (locate(substr(maskccusto,locate(maskccusto,separador)+1),separador)+1)))
                                                  FROM bethadba.parametros)as qtd_mask,
                                               right(repeat('0',qtd_maskOrgao)||trim(ccustos.nivel1),qtd_maskOrgao) as mascara_orgao,
                                               (select cast(max(cc.nivel2) as int) from bethadba.ccustos cc where cc.nivel1 = mascara_orgao) as maximo_unidade,
                                               right(repeat('0',qtd_maskUnidade)||trim(ccustos.nivel2),qtd_maskUnidade) as mascara_unidade,
                                               right(repeat('0',qtd_mask)||trim(ccustos.nivel3),qtd_mask) as mascara_ccusto,
                                               if( (right(repeat('0',qtd_maskOrgao)||trim(ccustos.nivel1),qtd_maskOrgao) = repeat('0',qtd_maskOrgao)) 
                                                   or (right(repeat('0',qtd_mask)||trim(ccustos.nivel3),qtd_mask) <> repeat('0',qtd_mask) 
                                                   and (right(repeat('0',qtd_maskUnidade)||trim(ccustos.nivel2),qtd_maskUnidade) = repeat('0',qtd_maskUnidade)))
                                               ) then 1 else 0 endif as nivelIncorreto,
                                               if(trim(ccustos.nivel1) = '' or trim(ccustos.nivel2) = '' or trim(ccustos.nivel3) = '')  then 1 else 0 endif nivelVazio,
                                               ccustos.i_ccusto as codCcusto,
                                               ccustos.i_ano_ccusto as anoCcusto,
                                               (select first maskccusto from bethadba.parametros) as mascaraCcusto,
                                               right(repeat('0',2)||trim(nivel1),2) || right(repeat('0',3)||trim(nivel2),3) || right(repeat('0',5)||trim(nivel3),5) as numeroCC
                                          from bethadba.ccustos 
                                         where not exists(select 1
                                                           from bethadba.config_ccustos config
                                                          where config.i_ano = ccustos.i_ano_ccusto
                                                            and right(repeat('0',2)||trim(config.i_orgaos),2) || right(repeat('0',3)||trim(config.i_unidades),3) || right(repeat('0',5)||trim(config.i_ccusto),5) =  numeroCC )
                                           and numeroCC in (select distinct mascara
                                                                      from (
                                                                            select right(repeat('0',2)||trim(ccustos.nivel1),2)||right(repeat('0',3)||trim(ccustos.nivel2),3)||right(repeat('0',5)||trim(ccustos.nivel3),5) as mascara
                                                                              from bethadba.posicao key join
                                                                                   bethadba.materiais key join
                                                                                   bethadba.movimentos key join
                                                                                   bethadba.entradas key join
                                                                                   bethadba.ccustos
                                                                            where posicao.saldofisico > 0
                                                                            
                                                                            UNION ALL
                                                                            
                                                                            select right(repeat('0',2)||trim(ccustos.nivel1),2)||right(repeat('0',3)||trim(ccustos.nivel2),3)||right(repeat('0',5)||trim(ccustos.nivel3),5) as mascara
                                                                              from bethadba.posicao key join
                                                                                   bethadba.materiais key join
                                                                                   bethadba.movimentos key join
                                                                                   bethadba.saidas key join
                                                                                   bethadba.ccustos
                                                                            where posicao.saldofisico > 0
                                                                           ) as masc)
                                           and right(repeat('0',5)||trim(nivel3),5) <> repeat('0',5)
                                           and (nivelIncorreto <> 0 or nivelVazio <> 0)
                                         order by anoCcusto, codCcusto
    
                                         """)

                if len(busca) > 0:
                    df = pd.DataFrame(busca)
                    for row in df.itertuples():
                        match row.mascara_unidade:
                            case '00':
                                novo_nivel2 = row.maximo_unidade + 1
                                dadoAlterado.append(f"Alterado nivel2 do Centro de Custo {row.codCcusto} ano {row.i_ano_ccusto} para {novo_nivel2}")
                                comandoUpdate += f"""UPDATE bethadba.ccustos set nivel2 = '{novo_nivel2}' where i_ccusto = {row.codCcusto} and i_ano_ccusto = {row.i_ano_ccusto} and i_entidades = {row.i_entidades};\n"""
                            case _:
                                print('Problema não identificado')
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_mascara_incorreta_nivel_zerado: {e}")
            return

        if mascara_incorreta_nivel_zerado:
            dado = analisa_mascara_incorreta_nivel_zerado()

            if corrigirErros and len(dado) > 0:
                corrige_mascara_incorreta_nivel_zerado(listDados=dado)


    if dadosList:
        analisa_corrige_mascara_incorreta_nivel_zerado(pre_validacao="mascara_incorreta_nivel_zerado")





