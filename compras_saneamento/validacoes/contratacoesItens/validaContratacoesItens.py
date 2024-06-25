from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import validaQtdItemSfMaiorQtdConsumoContrato
import math


def contratacoesItens(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                      corrigirErros=False,
                      saldo_item_diferente_distribuicao_saldo_entre_entidades=False,
                      item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo=False
                      ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_saldo_item_diferente_distribuicao_saldo_entre_entidades(pre_validacao):
        nomeValidacao = "Saldo de itens do contrato diferente da distribuição entre as entidades"

        def analisa_saldo_item_diferente_distribuicao_saldo_entre_entidades():
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

        def corrige_saldo_item_diferente_distribuicao_saldo_entre_entidades(log=False):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            listContratos = []
            listEntidades = []
            listItens = []

            if log:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select
                                                ci.i_entidades,
                                                ci.i_contratos,
                                                ci.i_item,
                                                ci.qtde_ctr quantidadeContrato,
                                                isnull(pepi.qtd_estimada, 0) quantidadeProcesso
                                            from
                                                compras.contratos_itens ci
                                                left join compras.processos_entidades_partic_itens pepi on ci.i_entidades_processo = pepi.i_entidades and ci.i_ano_proc = pepi.i_ano_proc and ci.i_processo = pepi.i_processo and ci.i_entidades = pepi.i_entidades_partic and ci.i_item = pepi.i_item
                                                join compras.processos p on ci.i_ano_proc = p.i_ano_proc and ci.i_processo = p.i_processo and ci.i_entidades_processo = p.i_entidades
                                                join compras.contratos c on ci.i_contratos = c.i_contratos and ci.i_entidades = c.i_entidades
                                            where quantidadeContrato > quantidadeProcesso
                                                and year(c.data_ass) >= 1900
                                                and p.processo_multientidade = 'S'
                                                and c.i_contratos_sup_compras is null
                                            """)
                for row in busca:
                    comandoUpdate += f"""UPDATE compras.compras.contratos_itens set qtde_ctr = {row['quantidadeProcesso']} where i_contratos = {row['i_contratos']} and i_entidades = {row['i_entidades']} and i_item = {row['i_item']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                if log:
                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função saldo_item_diferente_distribuicao_saldo_entre_entidades: {e}")
            return

        if saldo_item_diferente_distribuicao_saldo_entre_entidades:
            dado = analisa_saldo_item_diferente_distribuicao_saldo_entre_entidades()

            if corrigirErros and len(dado) > 0:
                corrige_saldo_item_diferente_distribuicao_saldo_entre_entidades(log=True)
            elif corrigirErros:
                corrige_saldo_item_diferente_distribuicao_saldo_entre_entidades()

    # Está função abrange a analise da inconsistência dos SQLs @gaSql2 e @gaSql3 do arqjob de pre-validações
    def analisa_corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo(pre_validacao):
        nomeValidacao = "Item vinculado a mais de um contrato com a soma das quantidades passando da quantidade estipulada do processo"

        def analisa_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo():
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

        def corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo(log=False):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            if log:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                resultado = banco.consultar(f"""select
                                                    ci.i_entidades_processo, 
                                                    ci.i_ano_proc,
                                                    ci.i_processo,
                                                    ip.i_item, 
                                                    ip.qtde_item,
                                                    sum(ci.qtde_ctr) qtde_ctr,
                                                    count(*) qtdContratos,
                                                    list(ci.i_contratos) as contratos
                                                from 
                                                    compras.contratos_itens ci
                                                    join compras.itens_processo ip 
                                                    on ci.i_entidades_processo = ip.i_entidades 
                                                    and ci.i_ano_proc = ip.i_ano_proc 
                                                    and ci.i_processo = ip.i_processo 
                                                    and ci.i_item_ctr = ip.i_item 
                                                    join compras.contratos c 
                                                    on ci.i_entidades = c.i_entidades 
                                                    and ci.i_contratos = c.i_contratos
                                                    join compras.processos 
                                                    on ci.i_entidades_processo = processos.i_entidades
                                                    and ci.i_ano_proc = processos.i_ano_proc
                                                    and ci.i_processo = processos.i_processo
                                                where
                                                    c.i_contratos_sup_compras is null
                                                    and processos.processo_multientidade = 'N'
                                                    and processos.controle_qtd_cred <> 2
                                                group by  ci.i_entidades_processo, ci.i_ano_proc, ci.i_processo, ip.i_item,  ip.qtde_item,  ci.i_item_ctr
                                                having qtde_ctr > qtde_item and qtdContratos > 1
                                            """)
                if len(resultado) > 0:
                    for row in resultado:
                        contratos = row['qtdContratos']
                        quantidadeItemProcesso = row['qtde_item']
                        listaContratos = row['contratos'].split(',')

                        newQuantidadeItemContrato = math.floor(quantidadeItemProcesso / contratos)

                        for contrato in listaContratos:
                            dadoAlterado.append(f"Alterado a quantidade do item {row['i_item']} para {newQuantidadeItemContrato} do contrato {contrato} da entidade {row['i_entidades_processo']}")
                            comandoUpdate += f"""UPDATE compras.compras.contratos_itens set qtde_ctr = {newQuantidadeItemContrato} where i_contratos = {contrato} and i_entidades = {row['i_entidades_processo']} and i_item = {row['i_item']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)
                validaQtdItemSfMaiorQtdConsumoContrato(banco)

                if log:
                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo: {e}")
            return

        if item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo:
            dado = analisa_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo()

            if corrigirErros and len(dado) > 0:
                # a correção será chamada independente se foi localizado erros na tabela de controle
                corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo(log=True)
            elif corrigirErros:
                corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo()

    analisa_corrige_saldo_item_diferente_distribuicao_saldo_entre_entidades(pre_validacao='saldo_item_diferente_distribuicao_saldo_entre_entidades')
    analisa_corrige_item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo(pre_validacao='item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo')
