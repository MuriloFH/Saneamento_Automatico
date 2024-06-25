from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import pandas as pd


def lancamentoDespesaItem(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                          corrigirErros=False,
                          valor_unitario_item_despesa_zero=False,
                          codigo_item_despesa_nulo=False,
                          codigo_material_duplicado=False
                          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = "lancamentoDespesaItem"

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_valor_unitario_item_despesa_zero(pre_validacao):
        nomeValidacao = "Valor unitário do item da despesa zerado"
        preValidacaoBanco = pre_validacao

        def analisa_valor_unitario_item_despesa_zero():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}'")

            dados = []

            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_valor_unitario_item_despesa_zero(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                i_movimentos = []
                try:
                    for dados in listDados:
                        i_movimentos.append(dados['i_chave_dsk1'])

                    i_movimentos = "','".join(i_movimentos)
                    busca = banco.consultar(f"""
                                            SELECT m.i_movimentos, 
                                            im.i_itens_mov,
                                            im.qtdade,
                                            im.valor
                                            from bethadba.movimentos m
                                            join bethadba.itens_movimento im on (im.i_movimentos = m.i_movimentos)
                                            WHERE m.i_movimentos in ('{i_movimentos}')
                                            """)

                    if len(busca) > 0:
                        for i in busca:
                            dadoAlterado.append(f"Alterado a quantidade e valor para 1 do item {i['i_itens_mov']} da despesa {i['i_movimentos']}")

                            comandoUpdate += f"update bethadba.itens_movimento set valor = 1, qtdade = 1 where i_movimentos = {i['i_movimentos']} and i_itens_mov = {i['i_itens_mov']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_valor_unitario_item_despesa_zero {e}")

        if valor_unitario_item_despesa_zero:
            dado = analisa_valor_unitario_item_despesa_zero()

            if corrigirErros and len(dado) > 0:
                corrige_valor_unitario_item_despesa_zero(listDados=dado)

    def valida_corrige_codigo_item_despesa_nulo(pre_validacao):
        nomeValidacao = "Codigo nulo do item da despesa"
        preValidacaoBanco = pre_validacao

        def analisa_codigo_item_despesa_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}'")

            dados = []

            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_item_despesa_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                newCodMaterial = banco.consultar("""select first i_materiais from bethadba.materiais""")

                i_movimentos = []
                try:
                    for dados in listDados:
                        i_movimentos.append(dados['i_chave_dsk1'])

                    i_movimentos = "','".join(i_movimentos)
                    busca = banco.consultar(f"""
                                                            SELECT m.i_movimentos, 
                                                            im.i_itens_mov,
                                                            im.qtdade,
                                                            im.valor
                                                            from bethadba.movimentos m
                                                            join bethadba.itens_movimento im on (im.i_movimentos = m.i_movimentos)
                                                            WHERE m.i_movimentos in ('{i_movimentos}')
                                                            """)

                    if len(busca) > 0:
                        for i in busca:
                            dadoAlterado.append(f"adicionado o material {newCodMaterial[0]['i_materiais']} para o item {i['i_itens_mov']} da despesa {i['i_movimentos']}")

                            comandoUpdate += f"update bethadba.itens_movimento set i_materiais = {newCodMaterial[0]['i_materiais']} where i_movimentos = {i['i_movimentos']} and i_itens_mov = {i['i_itens_mov']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_codigo_item_despesa_nulo {e}")

        if codigo_item_despesa_nulo:
            dado = analisa_codigo_item_despesa_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_item_despesa_nulo(listDados=dado)

    def valida_corrige_codigo_material_duplicado(pre_validacao):
        nomeValidacao = "Material duplicado na despesa"
        preValidacaoBanco = pre_validacao

        def analisa_codigo_material_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}'")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_material_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []

                try:
                    listDados = sorted(listDados, key=lambda x: x['i_chave_dsk1'])

                    df = pd.DataFrame(listDados)
                    dfLimpo = df.drop_duplicates(subset='i_chave_dsk1')
                    dfLimpo = dfLimpo.to_dict('records')

                    comandoUpdate = ""
                    comandoDelete = ""
                    for j in dfLimpo:
                        busca = banco.consultar(f'''
                                                SELECT list(i_movimentos) as i_movimentos,
                                                list(im.i_itens_mov) as i_itens_mov,
                                                list(i_materiais) as i_materias,
                                                sum(qtdade) as qtdade,
                                                sum(valor) as valor
                                                FROM bethadba.itens_movimento im
                                                WHERE (SELECT count(1) from bethadba.itens_movimento im2
                                                where im2.i_movimentos = im.i_movimentos and im2.i_materiais = im.i_materiais) > 1 and im.i_movimentos = {j['i_chave_dsk1']}
                                                group by im.i_movimentos, im.i_materiais
                                                ''')
                        for i in busca:
                            newItemMovimentacao = {
                                'i_movimentos': list(set(str(i['i_movimentos']).split(sep=','))),
                                'i_itens_mov': list(set(str(i['i_itens_mov']).split(sep=','))),
                                'i_materias': list(set(str(i['i_materias']).split(sep=','))),
                                'qtdade': str(i['qtdade']),
                                'valor': str(i['valor'])
                            }

                            dadoAlterado.append(f"Agrupado o material {newItemMovimentacao['i_materias'][0]} da movimentação {newItemMovimentacao['i_movimentos'][0]}")

                            # update
                            comandoUpdate += f"update bethadba.itens_movimento set qtdade = '{newItemMovimentacao['qtdade']}', valor = '{newItemMovimentacao['valor']}' where i_movimentos = '{newItemMovimentacao['i_movimentos'][0]}' and i_itens_mov = '{newItemMovimentacao['i_itens_mov'][0]}' and i_materiais = '{newItemMovimentacao['i_materias'][0]}';\n"

                            # delete
                            for itensMov in newItemMovimentacao['i_itens_mov'][1:]:
                                comandoDelete += f"delete from bethadba.itens_movimento where i_movimentos = '{newItemMovimentacao['i_movimentos'][0]}' and i_itens_mov = '{itensMov}' and i_materiais = '{newItemMovimentacao['i_materias'][0]}';\n"

                    # update
                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    # delete
                    banco.executar(comando=banco.triggerOff(comandoDelete))

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_codigo_material_duplicado {e}")

        if codigo_material_duplicado:
            dado = analisa_codigo_material_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_material_duplicado(listDados=dado)

    if dadosList:
        valida_corrige_valor_unitario_item_despesa_zero(pre_validacao='valor_unitario_item_despesa_zero')
        valida_corrige_codigo_item_despesa_nulo(pre_validacao='item_despesa_nulo')
        valida_corrige_codigo_material_duplicado(pre_validacao='codigo_material_duplicado')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
