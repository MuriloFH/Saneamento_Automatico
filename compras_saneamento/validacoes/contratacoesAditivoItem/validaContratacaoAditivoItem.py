import colorama

from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def contratacaoAditivoItem(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                           corrigirErros=False,
                           aditivo_sem_item=False,
                           aditivo_de_prazo_com_itens=False
                           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_aditivo_sem_item(pre_validacao):
        nomeValidacao = "Aditivo sem itens do contrato principal"

        def analisa_aditivo_sem_item():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_aditivo_sem_item(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dadosContratoPrincipal = banco.consultar(f"""select 
                                                            c.i_contratos_sup_compras as contrato,
                                                            list(c.i_contratos) as aditivo,
                                                            c.i_entidades,
                                                            count(c.i_contratos) as qtdAditivos
                                                            from compras.contratos c left join compras.contratos_itens ci 
                                                            where c.i_ano_proc >= 1900
                                                                and c.natureza in (2,4,6,7)
                                                                and contrato is not null
                                                            and ci.i_item_ctr is null
                                                            group by contrato, c.i_entidades
                                                            order by contrato
                                              """)
                listAditivos = []
                for contratoPrincipal in dadosContratoPrincipal:
                    listAditivos = contratoPrincipal['aditivo'].split(',')
                    # aditivos = ','.join(listAditivos)

                    buscaItens = banco.consultar(f"""SELECT i_entidades, i_item_ctr, i_material, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo
                                                    from compras.compras.contratos_itens ci 
                                                    where ci.i_contratos in ({contratoPrincipal['contrato']}) and ci.i_entidades = {contratoPrincipal['i_entidades']}
                                                """)
                    if len(buscaItens) > 0:
                        for aditivo in listAditivos:
                            for item in buscaItens:
                                if item['i_material'] is None:
                                    item['i_material'] = 'null'

                                dadoAlterado.append(f"Inserido o item {item['i_item']} no aditivo {aditivo} do contrato {contratoPrincipal['contrato']}.")
                                comandoInsert += f"""insert into compras.contratos_itens (i_contratos, i_entidades, i_item_ctr, i_material, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo)
                                                     values({aditivo}, {item['i_entidades']}, {item['i_item']}, {item['i_material']}, {item['qtde_ctr']}, {item['preco_unit_ctr']}, {item['preco_total_ctr']}, {item['i_ano_proc']}, {item['i_processo']}, {item['i_item']}, {item['i_entidades']});\n"""
                    else:
                        print(colorama.Fore.RED, f"Não localizado nenhum item do contrato {contratoPrincipal['contrato']} da entidade {contratoPrincipal['i_entidades']} pois o mesmo pode ser oriundo de Processo Licitatório, Compra Direta ou não foi ajustado na validação 'Contrato sem item'.", colorama.Fore.RESET)

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_aditivo_sem_item: {e}")
            return

        if aditivo_sem_item:
            dado = analisa_aditivo_sem_item()

            if corrigirErros and len(dado) > 0:
                corrige_aditivo_sem_item(listDados=dado)

    def analisa_corrige_aditivo_de_prazo_com_itens(pre_validacao):
        nomeValidacao = "Aditivo de prazo com itens"

        def analisa_aditivo_de_prazo_com_itens():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_aditivo_de_prazo_com_itens(listDados):
            tipoCorrecao = "REMOCAO"
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                resultado = banco.consultar(f"""SELECT co.i_entidades AS i_chave_dsk1,
                                                        co.i_contratos AS i_chave_dsk2,
                                                        count(i_item_ctr) AS i_chave_dsk3,
                                                        co.i_contratos_sup_compras AS i_chave_dsk4,
                                                        list(ic.i_item_ctr) as itensContratos 
                                                        FROM compras.contratos co 
                                                        JOIN compras.contratos_itens ic ON co.i_contratos = ic.i_contratos AND co.i_entidades = ic.i_entidades 
                                                        WHERE 
                                                            i_contratos_sup_compras IS NOT NULL 
                                                            AND natureza = 3 -- Natureza de Aditivo de Prazo
                                                        GROUP BY (co.i_contratos, co.i_contratos_sup_compras, co.i_entidades)
                                            """)
                for row in resultado:
                    listItensAditivo = row['itensContratos'].split(',')
                    for item in listItensAditivo:
                        dadoAlterado.append(f"Removido o item {item} do aditivo {row['i_chave_dsk2']} do contrato {row['i_chave_dsk4']} da entidade {row['i_chave_dsk1']}")
                        comandoDelete += f"""DELETE from compras.compras.contratos_itens where i_item_ctr = {item} and i_contratos = {row['i_chave_dsk2']} and i_entidades = {row['i_chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_aditivo_de_prazo_com_itens: {e}")
            return

        if aditivo_de_prazo_com_itens:
            dado = analisa_aditivo_de_prazo_com_itens()

            if corrigirErros and len(dado) > 0:
                corrige_aditivo_de_prazo_com_itens(listDados=dado)

    if dadosList:
        analisa_corrige_aditivo_sem_item(pre_validacao='aditivo_sem_item')
        analisa_corrige_aditivo_de_prazo_com_itens(pre_validacao='aditivo_de_prazo_com_itens')
