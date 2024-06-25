import random
import pandas as pd
from utilitarios.funcoesGenericas.funcoes import getDadosSolicitacoesFornecimentoValidacao, getDadosDataHomologacaoProcessoMaiorDataSf, adicionaDiasData, geraDataSfMaiorQueDataHomologacao, getItensContratos
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def solicitacaoFornecimento(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                            corrigirErros=False,
                            data_sf_menor_data_inicio_contrato=False,
                            data_sf_menor_data_fim_contrato=False,
                            data_homologacao_processo_maior_data_sf=False,
                            sf_sem_contrato_informado=False
                            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_sf_menor_data_inicio_contrato(pre_validacao):
        nomeValidacao = "Data da SF menor que a data de inicio do contrato"

        def analisa_data_sf_menor_data_inicio_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_sf_menor_data_inicio_contrato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = getDadosSolicitacoesFornecimentoValidacao(conexaoBanco=banco, validacao='data_sf_menor_data_inicio_contrato')

                for i in dados:
                    inicioVigenciaContrato = i['data_inicio_vigencia']
                    fimVigenciaContrato = i['data_fim_vigencia']
                    if inicioVigenciaContrato == fimVigenciaContrato:
                        newDataSF = inicioVigenciaContrato
                    else:
                        qtdDias = fimVigenciaContrato - inicioVigenciaContrato
                        # Feito o while pois pode acontecer de o randDias dar 365 e colocar 1 dia amais que a data fim
                        while True:
                            randDias = random.randint(1, qtdDias.days)
                            newDataSF = adicionaDiasData(data=inicioVigenciaContrato, qtdDias=randDias)
                            if newDataSF >= inicioVigenciaContrato or newDataSF <= fimVigenciaContrato:
                                break

                    dadoAlterado.append(f"""Alterado a data da Af para {newDataSF} do processo {i['i_processo']}/{i['i_ano_proc']} da entidade {i['i_entidades_partic']}""")
                    comandoUpdate += f"""UPDATE compras.compras.adjudicacao adj set adj.data_af = '{newDataSF}' , adj.data_adjudica = '{newDataSF}'
                                            where adj.i_sequ_adj = {i['i_sequ_adj']}
                                            and (adj.data_af = '{i['data_SF']}' or adj.data_adjudica = '{i['data_SF']}')
                                            and adj.i_ano_proc = {i['i_ano_proc']}
                                            and adj.i_processo = {i['i_processo']}
                                            and adj.i_entidades = {i['i_entidades']};\n
                                     """

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_sf_menor_data_inicio_contrato: {e}")
            return

        if data_sf_menor_data_inicio_contrato:
            dado = analisa_data_sf_menor_data_inicio_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_data_sf_menor_data_inicio_contrato(listDados=dado)

    def analisa_corrige_data_sf_menor_data_fim_contrato(pre_validacao):
        nomeValidacao = "Data da SF menor que a data de fim do contrato"

        def analisa_data_sf_menor_data_fim_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_sf_menor_data_fim_contrato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = getDadosSolicitacoesFornecimentoValidacao(conexaoBanco=banco, validacao='data_sf_menor_data_fim_contrato')

                for i in dados:
                    inicioVigenciaContrato = i['data_inicio_vigencia']
                    fimVigenciaContrato = i['data_fim_vigencia']
                    if inicioVigenciaContrato == fimVigenciaContrato:
                        newDataSF = inicioVigenciaContrato
                    else:
                        qtdDias = fimVigenciaContrato - inicioVigenciaContrato
                        # Feito o while pois pode acontecer de o randDias dar 365 e colocar 1 dia amais que a data fim
                        while True:
                            randDias = random.randint(1, qtdDias.days)
                            newDataSF = adicionaDiasData(data=inicioVigenciaContrato, qtdDias=randDias)
                            if newDataSF >= inicioVigenciaContrato or newDataSF <= fimVigenciaContrato:
                                break

                    dadoAlterado.append(f"""Alterado a data da Af para {newDataSF} do processo {i['i_processo']}/{i['i_ano_proc']} da entidade {i['i_entidades_partic']}""")
                    comandoUpdate += f"""UPDATE compras.compras.adjudicacao adj set adj.data_af = '{newDataSF}' , adj.data_adjudica = '{newDataSF}'
                                            where adj.i_sequ_adj = {i['i_sequ_adj']}
                                            and (adj.data_af = '{i['data_SF']}' or adj.data_adjudica = '{i['data_SF']}')
                                            and adj.i_ano_proc = {i['i_ano_proc']}
                                            and adj.i_processo = {i['i_processo']}
                                            and adj.i_entidades = {i['i_entidades']};\n
                                     """

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_sf_menor_data_fim_contrato: {e}")
            return

        if data_sf_menor_data_fim_contrato:
            dado = analisa_data_sf_menor_data_fim_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_data_sf_menor_data_fim_contrato(listDados=dado)

    def analisa_corrige_data_homologacao_processo_maior_data_sf(pre_validacao):
        nomeValidacao = "Data da homologação do processo maior que a data da SF"

        def analisa_data_homologacao_processo_maior_data_sf():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_homologacao_processo_maior_data_sf(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = getDadosDataHomologacaoProcessoMaiorDataSf(conexaoBanco=banco)

                for i in dados:
                    dataHomologacao = i['data_homologacao']
                    inicioVigencia = pd.to_datetime(i['data_inicio_vigencia']).date()
                    fimVigencia = pd.to_datetime(i['data_fim_vigencia']).date()

                    if i['chave_dsk6'] == 2:
                        i_entidade = i['chave_dsk1']
                        i_sequ_adit = i['chave_dsk5']
                        i_contratos = i['chave_dsk2']
                        i_item_adit = i['i_item_adit']

                        newDataAdit = geraDataSfMaiorQueDataHomologacao(inicioVigencia=inicioVigencia, fimVigencia=fimVigencia, dataHomologacao=dataHomologacao)

                        if newDataAdit != "":
                            dadoAlterado.append(f"Alterado a data_adit do aditamento {i_sequ_adit} do contrato {i_contratos} da entidade {i_entidade}")
                            comandoUpdate += f"""UPDATE compras.compras.aditamento adit set adit.data_adit = '{newDataAdit}'
                                                    where adit.i_entidades = {i_entidade}
                                                    and adit.i_sequ_adit = {i_sequ_adit}
                                                    and adit.i_contratos = {i_contratos}
                                                    and adit.i_item_adit  = {i_item_adit};\n
                                            """
                        else:
                            continue

                    if i['chave_dsk6'] == 1:
                        i_sequ_adj = i['chave_dsk5']
                        data_SF = i['data_SF']
                        i_ano_proc = i['chave_dsk2']
                        i_processo = i['chave_dsk3']
                        i_entidades = i['i_entidades']

                        newDataSf = geraDataSfMaiorQueDataHomologacao(inicioVigencia=inicioVigencia, fimVigencia=fimVigencia, dataHomologacao=dataHomologacao)

                        if newDataSf != "":
                            dadoAlterado.append(f"Alterado a data_af da adjudicacao {i_sequ_adj} do processo {i_processo}/{i_ano_proc} da entidade {i_entidades}")
                            comandoUpdate += f"""UPDATE compras.compras.adjudicacao adj set adj.data_af = '{newDataSf}' , adj.data_adjudica = '{newDataSf}'
                                                    where adj.i_sequ_adj = {i_sequ_adj}
                                                    and (adj.data_af = '{data_SF}' or adj.data_adjudica = '{data_SF}')
                                                    and adj.i_ano_proc = {i_ano_proc}
                                                    and adj.i_processo = {i_processo}
                                                    and adj.i_entidades = {i_entidades};\n
                                             """
                        else:
                            continue

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_homologacao_processo_maior_data_sf: {e}")
            return

        if data_homologacao_processo_maior_data_sf:
            dado = analisa_data_homologacao_processo_maior_data_sf()

            if corrigirErros and len(dado) > 0:
                corrige_data_homologacao_processo_maior_data_sf(listDados=dado)

    def analisa_corrige_sf_sem_contrato_informado(pre_validacao):
        nomeValidacao = "SF sem contrato informado"

        def analisa_sf_sem_contrato_informado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_sf_sem_contrato_informado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            listChave1 = []
            listChave2 = []
            listChave3 = []
            listChave4 = []
            listChave5 = []
            listChave6 = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:

                for i in listDados:
                    listChave1.append(i['i_chave_dsk1'])
                    listChave2.append(i['i_chave_dsk2'])
                    listChave3.append(i['i_chave_dsk3'])
                    listChave4.append(i['i_chave_dsk4'])
                    listChave5.append(i['i_chave_dsk5'])
                    listChave6.append(i['i_chave_dsk6'])

                listChave1 = ','.join(listChave1)
                listChave2 = ','.join(listChave2)
                listChave3 = ','.join(listChave3)
                listChave4 = ','.join(listChave4)
                listChave5 = ','.join(listChave5)
                listChave6 = ','.join(listChave6)

                busca = banco.consultar(f"""select
                                                sa.i_entidades as i_entidades, sa.i_ano_proc as i_ano_proc, sa.i_processo as i_processo, sa.i_credores as i_credores, isnull(string(sa.i_sequ_adj), '') as i_sequ_adj,
                                                 1 as chave_dsk6, aux.i_entidades_processo entidadeProcesso, isnull(sa.i_contratos, '') as contratoSF, sa.i_ano_aut as anoSF, sa.i_sequ_aut as sequenciaSF, sa.data_aut
                                            from 
                                                compras.sequ_autor sa 
                                                join (select c.i_entidades_processo, c.i_ano_proc, c.i_processo, c.i_credores, count(*) qtdContratos
                                                        from 
                                                            compras.contratos c
                                                            join compras.processos on c.i_entidades_processo = processos.i_entidades and c.i_ano_proc = processos.i_ano_proc  and c.i_processo = processos.i_processo 
                                                        where 
                                                            c.i_contratos_sup_compras is null
                                                        group by  
                                                            c.i_entidades_processo, 
                                                            c.i_ano_proc,
                                                            c.i_processo,
                                                            c.i_credores
                                                        having count(*) > 1) aux on sa.i_entidades_partic = aux.i_entidades_processo and sa.i_ano_proc = aux.i_ano_proc and sa.i_processo = aux.i_processo and sa.i_credores = aux.i_credores
                                                join compras.processos on aux.i_entidades_processo = processos.i_entidades and aux.i_ano_proc = processos.i_ano_proc and aux.i_processo = processos.i_processo
                                            where i_entidades in ({listChave1})
                                                and i_ano_proc in ({listChave2})
                                                and i_processo in ({listChave3})
                                                and i_credores in ({listChave4})
                                                and i_sequ_adj in ({listChave5})
                                                and chave_dsk6 in ({listChave6})
                                            order by i_ano_aut desc, i_sequ_aut desc
                                        """)

                for row in busca:
                    comandoDelete = ""
                    comandoUpdate = ""
                    comandoUpdateSf = ""
                    comandoInsert = ""
                    # fazer uma busca nos contratos que retornarem do processo dessa SF
                    buscaContrato = banco.consultar(f"""SELECT c.i_contratos
                                                        from compras.compras.contratos c 
                                                        where c.i_processo = {row['i_processo']}
                                                        and c.i_ano_proc = {row['i_ano_proc']}
                                                        and c.i_entidades = {row['i_entidades']}
                                                        and c.i_credores = {row['i_credores']}
                                                    """)

                    # reservo o primeiro contrato retornado para utilizar posteriormente no script
                    contratoParaSf = buscaContrato[0]
                    itensContratoParaSf = getItensContratos(banco, i_contratos=contratoParaSf['i_contratos'], i_entidades=row['i_entidades'])

                    # depois percorrer cada contrato e coletar todos os itens dos mesmos
                    for contrato in buscaContrato[1:]:
                        comandoInsert = ""
                        comandoDelete = ""
                        buscaItensContrato = getItensContratos(banco, i_contratos=contrato['i_contratos'], i_entidades=row['i_entidades'])

                        # proximo passo é excluir cada item, pois vamos inserir novamente porém com os itens do contrato reservado
                        for item in buscaItensContrato:
                            comandoDelete += f"DELETE FROM compras.compras.contratos_itens WHERE i_contratos = {item['i_contratos']} and i_entidades = {item['i_entidades']} and i_item_ctr = {item['i_item_ctr']};\n"

                        # depois de gerar todos os delete dos itens do contrato, executa os comandos.
                        banco.executar(banco.triggerOff(comandoDelete))

                        # após isso será percorrido os itens do contrato reservado e inserido no contrato que teve os itens excluidos, porem com um detalhe, sem o ultimo item.
                        for itemContratoSf in itensContratoParaSf[:(len(itensContratoParaSf) - 1)]:
                            comandoInsert += f"""insert into compras.contratos_itens (i_contratos, i_entidades, i_item_ctr, i_material, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo)
                                                                                values({contrato['i_contratos']}, {itemContratoSf['i_entidades']}, {itemContratoSf['i_item']}, {itemContratoSf['i_material']}, {itemContratoSf['qtde_ctr']}, {itemContratoSf['preco_unit_ctr']}, {itemContratoSf['preco_total_ctr']}, {itemContratoSf['i_ano_proc']}, {itemContratoSf['i_processo']}, {itemContratoSf['i_item']}, {itemContratoSf['i_entidades']});\n"""

                        # depois de gerar todos os insert dos itens do contrato, executa os comandos
                        banco.executar(banco.triggerOff(comandoInsert))

                    # depois de reinserir os itens nos contratos, será adicionado o id do contrato principal na SF
                    dadoAlterado.append(f"Inserido o contrato {contratoParaSf['i_contratos']} para a SF {row['sequenciaSF']} do ano {row['anoSF']} da entidade {row['i_entidades']}")
                    comandoUpdateSf += f"UPDATE compras.compras.sequ_autor set i_contratos = {contratoParaSf['i_contratos']} where i_ano_aut = '{row['anoSF']}' and i_sequ_aut = {row['sequenciaSF']} and i_entidades = {row['i_entidades']};\n"
                    banco.executar(banco.triggerOff(comandoUpdateSf))

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_sf_sem_contrato_informado: {e}")
            return

        if sf_sem_contrato_informado:
            dado = analisa_sf_sem_contrato_informado()

            if corrigirErros and len(dado) > 0:
                corrige_sf_sem_contrato_informado(listDados=dado)

    if dadosList:
        analisa_corrige_data_sf_menor_data_inicio_contrato(pre_validacao='data_sf_menor_data_inicio_contrato')
        analisa_corrige_data_sf_menor_data_fim_contrato(pre_validacao='data_sf_menor_data_fim_contrato')
        analisa_corrige_data_homologacao_processo_maior_data_sf(pre_validacao='data_homologacao_processo_maior_data_sf')
        analisa_corrige_sf_sem_contrato_informado(pre_validacao='sf_sem_contrato_informado')
