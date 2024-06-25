from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def prazosEntrega(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  prazos_entrega_maior_quatro_digitos=False,
                  descricao_prazos_entrega_maior_cinquenta_digitos=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_prazos_entrega_maior_quatro_digitos(pre_validacao):
        nomeValidacao = "Não é permitido mais de 4 dígitos no campo Prazo de entrega!"

        def analisa_prazos_entrega_maior_quatro_digitos():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_prazos_entrega_maior_quatro_digitos(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select distinct 
                                                isnull(i_entidades,1) as i_entidades,
                                                i_processo,
                                                i_ano_proc,
                                                if (prazo_entrega is null or numDiasMeses = 0 or numDiasMeses is null) then 'OUTROS' else (case tipo_prazo_entrega when 1 then 'DIAS' when 2 then 'MESES' Else 'OUTROS' end) endif as unidadeEntrega,
                                                numDiasMesesTemp = if tipo_prazo_entrega <> 3 then cast(isnull(trim(compras.dbf_retira_caracteres(prazo_entrega)),0) as integer) endif,
                                                case isnull(tipo_prazo_entrega,3) 
                                                    when 3 then 0 
                                                    else isnull(numDiasMesesTemp,0)
                                                end as numDiasMeses,
                                                case 
                                                    when LENGTH(numDiasMeses) > 4 then RIGHT(numDiasMeses,4)
                                                    else numDiasMeses
                                                end as novo_numDiasMeses,
                                                prazo_entrega
                                            from compras.processos
                                            where LENGTH(numDiasMeses) > 4 and unidadeEntrega = 'DIAS'    
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado prazo de entrega do processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']} de {row['prazo_entrega']} para {int(row['novo_numDiasMeses'])} DIAS")
                        comandoUpdate += f"""update compras.processos set prazo_entrega = '{int(row['novo_numDiasMeses'])} DIAS' where i_processo = {row['i_processo']} and i_ano_proc = {row['i_ano_proc']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_prazos_entrega_maior_quatro_digitos: {e}")
            return

        if prazos_entrega_maior_quatro_digitos:
            dado = analisa_prazos_entrega_maior_quatro_digitos()

            if corrigirErros and len(dado) > 0:
                corrige_prazos_entrega_maior_quatro_digitos(listDados=dado)

    def analisa_corrige_descricao_prazos_entrega_maior_cinquenta_digitos(pre_validacao):
        nomeValidacao = "Não é permitido mais de 50 dígitos na descrição do Prazo de entrega!"

        def analisa_descricao_prazos_entrega_maior_cinquenta_digitos():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_prazos_entrega_maior_cinquenta_digitos(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" SELECT
                                            p.prazo_entrega,
                                            p.i_ano_proc,
                                            p.i_processo,
                                            p.i_entidades,
                                            p.modalidade
                                            from compras.compras.processos p 
                                            where LENGTH(p.prazo_entrega) > 49
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        newDescricao = row['prazo_entrega'][:49]
                        dadoAlterado.append(f"Alterado a descricao do prazo de entrega do processo {row['i_processo']}/{row['i_ano_proc']} entidade {row['i_entidades']} para {newDescricao}")
                        comandoUpdate += f"""update compras.processos set prazo_entrega = '{newDescricao}' where i_processo = {row['i_processo']} and i_ano_proc = {row['i_ano_proc']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_prazos_entrega_maior_cinquenta_digitos: {e}")
            return

        if descricao_prazos_entrega_maior_cinquenta_digitos:
            dado = analisa_descricao_prazos_entrega_maior_cinquenta_digitos()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_prazos_entrega_maior_cinquenta_digitos(listDados=dado)

    if dadosList:
        analisa_corrige_prazos_entrega_maior_quatro_digitos(pre_validacao="prazos_entrega_maior_quatro_digitos")
        analisa_corrige_descricao_prazos_entrega_maior_cinquenta_digitos(pre_validacao='descricao_prazos_entrega_maior_cinquenta_digitos')
