from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl
import colorama


def ataRegistroPreco(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                     corrigirErros=False,
                     ata_registro_preco_sem_processo_homologado=False,
                     ano_data_ata_diferente_ano_data_registro_preco=False,
                     fornecedor_ata_diferente_fornecedor_vencedor_processo=False
                     ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_ata_registro_preco_sem_processo_homologado(pre_validacao):
        nomeValidacao = "É necessário que o Processo Administrativo esteja homologado para inserir uma ata de registro de preço"

        def analisa_ata_registro_preco_sem_processo_homologado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ata_registro_preco_sem_processo_homologado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select DISTINCT 
                                                (select min(c.data_coleta) 
                                                    from compras.coletas c
                                                    where p.i_ano_proc = c.i_ano_proc and p.i_processo =  c.i_processo and p.i_entidades = c.i_entidades ) as data_coleta,
                                                p.i_ano_proc, 
                                                p.i_processo, 
                                                p.data_homolog, 
                                                p.reg_prec,
                                                p.data_abertura,
                                                p.data_recebimento,
                                                p.i_entidades
                                            from compras.processos p 
                                            where p.data_homolog is null
                                            and p.reg_prec = 'S'    
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada prazo de entrega do processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']} para  {row['data_coleta']}")
                        comandoUpdate += f"""update compras.processos set data_homolog = '{row['data_coleta']}', data_abertura = '{row['data_coleta']}', data_recebimento = '{row['data_coleta']}' where i_processo = {row['i_processo']} and i_ano_proc = {row['i_ano_proc']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ata_registro_preco_sem_processo_homologado: {e}")
            return

        if ata_registro_preco_sem_processo_homologado:
            dado = analisa_ata_registro_preco_sem_processo_homologado()

            if corrigirErros and len(dado) > 0:
                corrige_ata_registro_preco_sem_processo_homologado(listDados=dado)

    def analisa_corrige_ano_data_ata_diferente_ano_data_registro_preco(pre_validacao):
        nomeValidacao = "O ano da data da ata deve ser igual ao ano da data do registro de preço."

        def analisa_ano_data_ata_diferente_ano_data_registro_preco():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ano_data_ata_diferente_ano_data_registro_preco(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select 
                                               i_coleta, 
                                               i_ano_coleta, 
                                               i_entidades,
                                               data_coleta,
                                               i_ano_coleta,
                                               i_ano_coleta || RIGHT(data_coleta,6) as nova_data
                                            from compras.coletas 
                                            where year(data_coleta) <> i_ano_coleta    
                                            """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterada data_coleta da Ata de registro de preço {row['i_coleta']}/{row['i_ano_coleta']} entidade {row['i_entidades']} de {row['data_coleta']} para  {row['nova_data']}")
                        comandoUpdate += f"""update compras.coletas set data_coleta = '{row['nova_data']}' where i_coleta = {row['i_coleta']} and i_ano_coleta = {row['i_ano_coleta']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ano_data_ata_diferente_ano_data_registro_preco: {e}")
            return

        if ano_data_ata_diferente_ano_data_registro_preco:
            dado = analisa_ano_data_ata_diferente_ano_data_registro_preco()

            if corrigirErros and len(dado) > 0:
                corrige_ano_data_ata_diferente_ano_data_registro_preco(listDados=dado)

    def analisa_corrige_fornecedor_ata_diferente_fornecedor_vencedor_processo(pre_validacao):
        nomeValidacao = "Fornecedor vencedor do processo, diferente do fornecedor da ATA"

        def analisa_fornecedor_ata_diferente_fornecedor_vencedor_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fornecedor_ata_diferente_fornecedor_vencedor_processo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    newCredorColeta = i['i_chave_dsk2']
                    oldCredorColeta = i['i_chave_dsk3']
                    anoColeta = i['i_chave_dsk7']
                    coleta = i['i_chave_dsk8']
                    itemColeta = i['i_chave_dsk6']
                    entidade = i['i_chave_dsk1']
                    processo = i['i_chave_dsk5']
                    anoProcesso = i['i_chave_dsk4']

                    dadosOldCredor = banco.consultar(f"""SELECT *
                                                        from compras.itens_coletas
                                                        where i_ano_coleta = {anoColeta} and i_coleta = {coleta} and i_credores = {oldCredorColeta} and i_item_coleta = {itemColeta} and i_entidades = {entidade};
                                                    """)
                    if len(dadosOldCredor) > 0:
                        dadosOldCredor = dadosOldCredor[0]
                    else:
                        print(colorama.Fore.RED, f"Não localizado dados da ata do credor {oldCredorColeta} do item {itemColeta} da entidade {entidade} do ano {anoColeta}", colorama.Fore.RESET)
                        continue

                    dadosNewCredor = banco.consultar(f"""
                                                        SELECT *
                                                        from compras.itens_coletas
                                                        where i_ano_coleta = {anoColeta} and i_coleta = {coleta} and i_credores = {newCredorColeta} and i_item_coleta = {itemColeta} and i_entidades = {entidade};
                                                    """)

                    if len(dadosNewCredor) > 0:
                        dadosNewCredor = dadosNewCredor[0]
                    else:
                        print(colorama.Fore.RED, f"Não localizado dados da ata do credor {newCredorColeta} da entidade {entidade}", colorama.Fore.RESET)
                        continue

                    # alterando os dados do oldCredor para os do newCredor
                    comandoUpdate += f"""UPDATE compras.itens_coletas set qtde_coleta = {dadosNewCredor['qtde_coleta']} , preco_unit_coleta = {dadosNewCredor['preco_unit_coleta']}, preco_total_coleta = {dadosNewCredor['preco_total_coleta']}, comprar_item = 'N'
                                            where i_ano_coleta = {anoColeta} and i_coleta = {coleta} and i_credores = {oldCredorColeta} and i_item_coleta = {itemColeta} and i_entidades = {entidade};\n"""

                    # alterando os dados do newCredor para os do oldCredor
                    comandoUpdate += f"""UPDATE compras.itens_coletas set qtde_coleta = {dadosOldCredor['qtde_coleta']} , preco_unit_coleta = {dadosOldCredor['preco_unit_coleta']}, preco_total_coleta = {dadosOldCredor['preco_total_coleta']}, comprar_item = 'S'
                                                where i_ano_coleta = {anoColeta} and i_coleta = {coleta} and i_credores = {newCredorColeta} and i_item_coleta = {itemColeta} and i_entidades = {entidade};\n"""

                    dadoAlterado.append(f"Alterado o credor da ATA para {newCredorColeta} do processo {processo}/{anoProcesso}.")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_ata_diferente_fornecedor_vencedor_processo: {e}")
            return

        if fornecedor_ata_diferente_fornecedor_vencedor_processo:
            dado = analisa_fornecedor_ata_diferente_fornecedor_vencedor_processo()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_ata_diferente_fornecedor_vencedor_processo(listDados=dado)

    if dadosList:
        analisa_corrige_ata_registro_preco_sem_processo_homologado(pre_validacao="ata_registro_preco_sem_processo_homologado")
        analisa_corrige_ano_data_ata_diferente_ano_data_registro_preco(pre_validacao="ano_data_ata_diferente_ano_data_registro_preco")
        analisa_corrige_fornecedor_ata_diferente_fornecedor_vencedor_processo(pre_validacao='fornecedor_ata_diferente_fornecedor_vencedor_processo')
