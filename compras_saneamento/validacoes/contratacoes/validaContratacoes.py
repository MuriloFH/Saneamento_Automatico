from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import getDadosItenContrato, limpaItensContratos
import colorama


def contratacoes(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 contrato_sem_itens=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_contrato_sem_itens(pre_validacao):
        nomeValidacao = "Contrato sem item"

        def analisa_contrato_sem_itens():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_contrato_sem_itens(listDados):
            tipoCorrecao = "INSERSAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:

                for i in listDados:
                    limpaItensContratos(i_contratos=i['i_chave_dsk2'], i_entidades=i['i_chave_dsk1'], banco=banco)
                    comandoUpdate = ""
                    comandoInsert = ""
                    # passo 1 - realiza a busca dos itens do processo do respectivo contrato
                    busca = getDadosItenContrato(i_chave_dsk1=i['i_chave_dsk1'], i_chave_dsk2=i['i_chave_dsk2'], banco=banco)

                    # passo 2 - caso encontre itens do participante com situação 2, cria os insert, caso contrário retorna vazio.
                    if len(busca) > 0:
                        for row in busca:
                            if row['preco_total_ctr'] is None:
                                row['preco_total_ctr'] = 0

                            if row['i_material'] is None:
                                row['i_material'] = 'null'

                            dadoAlterado.append(f"Inserido o item {row['i_item']} no contrato {row['i_contratos']}")
                            comandoInsert += f"""insert into compras.contratos_itens (i_contratos, i_entidades, i_item_ctr, i_material, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo)
                                                    values({row['i_contratos']}, {row['i_entidades']}, {row['i_item']}, {row['i_material']}, {row['qtde_ctr']}, {row['preco_unit_ctr']}, {row['preco_total_ctr']}, {row['i_ano_proc']}, {row['i_processo']}, {row['i_item']}, {row['i_entidades']});\n"""
                    else:
                        # Caso retorne vazio o passo 2, entra no else e busca o credor que está no contrato
                        i_credor = banco.consultar(f"""SELECT p.i_ano_proc, p.i_processo, p.i_credores, p.i_entidades, p.situacao
                                                        from compras.compras.contratos c
                                                        join compras.compras.participantes p on (p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo and p.i_entidades = c.i_entidades and p.i_credores = c.i_credores)
                                                        where c.i_contratos = {i['i_chave_dsk2']} and c.i_entidades = {i['i_chave_dsk1']}
                                                    """)
                        if len(i_credor) > 0:
                            i_credor = i_credor[0]

                            # altera a situação do credor do contrato para 2
                            comandoUpdate += f"""UPDATE compras.compras.participantes set situacao = 2 
                                                    where i_ano_proc = {i_credor['i_ano_proc']} and i_processo = {i_credor['i_processo']} and i_credores = {i_credor['i_credores']} and i_entidades = {i_credor['i_entidades']};\n"""

                            # depois busca todos os outros participantes do processo vinculado ao contrato que estejam com a situação 2, menos o credor presente no contrato
                            buscaParticipantes = banco.consultar(f"""SELECT distinct p.i_credores, c.i_contratos, p.i_ano_proc, p.i_processo, p.i_entidades, p.situacao
                                                                        from compras.compras.contratos c
                                                                        join compras.compras.participantes p on (p.i_ano_proc = c.i_ano_proc and p.i_processo = c.i_processo and p.i_entidades = c.i_entidades)
                                                                        where c.i_contratos = {i['i_chave_dsk2']} and c.i_entidades = {i['i_chave_dsk1']} and p.i_credores <> {int(i_credor['i_credores'])} and p.situacao = 2;
                                                                """)

                            # caso encontre algum, altera a situação para 0 dos respectivos participantes
                            if len(buscaParticipantes) > 0:
                                for participantes in buscaParticipantes:
                                    comandoUpdate += f"""UPDATE compras.compras.participantes set situacao = 4 
                                                            where i_ano_proc = {participantes['i_ano_proc']} and i_processo = {participantes['i_processo']} and i_credores = {participantes['i_credores']} and i_entidades = {participantes['i_entidades']};\n"""

                            # Depois busca os itens do contrato com base no id do credor presente no contrato.
                            busca = getDadosItenContrato(i_chave_dsk1=i['i_chave_dsk1'], i_chave_dsk2=i['i_chave_dsk2'], idCredor=i_credor['i_credores'], banco=banco)

                            for row in busca:
                                # Aqui é criado os insert dos itens para aqueles contratos que não tinham o credor com a situação 2
                                if row['preco_total_ctr'] is None:
                                    row['preco_total_ctr'] = 0

                                if row['i_material'] is None:
                                    row['i_material'] = 'null'

                                dadoAlterado.append(f"Inserido o item {row['i_item']} no contrato {row['i_contratos']}")
                                comandoInsert += f"""insert into compras.contratos_itens (i_contratos, i_entidades, i_item_ctr, i_material, qtde_ctr, preco_unit_ctr, preco_total_ctr, i_ano_proc, i_processo, i_item, i_entidades_processo)
                                                        values({row['i_contratos']}, {row['i_entidades']}, {row['i_item']}, {row['i_material']}, {row['qtde_ctr']}, {row['preco_unit_ctr']}, {row['preco_total_ctr']}, {row['i_ano_proc']}, {row['i_processo']}, {row['i_item']}, {row['i_entidades']});\n"""

                        else:
                            print(colorama.Fore.RED, f"""O credor informado no contrato {i['i_chave_dsk2']} da entidade {i['i_chave_dsk1']}, não é o mesmo credor vencedor do processo. O contrato não será ajustado. Favor revisar o contrato e informar o credor correto.\n OBS: Caso tenha alguma validação que necessite dos itens do contrato, a inconsistência não será ajustada.""", colorama.Fore.RESET)
                            continue
                    # passo 3 - executa os comandos necessários para o banco
                    banco.executar(banco.triggerOff(comandoUpdate))
                    banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função contrato_sem_itens: {e}")
            return

        if contrato_sem_itens:
            dado = analisa_contrato_sem_itens()

            if corrigirErros and len(dado) > 0:
                corrige_contrato_sem_itens(listDados=dado)

    if dadosList:
        analisa_corrige_contrato_sem_itens(pre_validacao='contrato_sem_itens')
