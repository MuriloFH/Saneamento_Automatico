from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def processoAdmItemReservado(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       item_cota_reservada_processo_configuracao_exclusividade=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_item_cota_reservada_processo_configuracao_exclusividade(pre_validacao):
        nomeValidacao = "Item do processo configurado com o campo Só MPEs no processo, mais o processo não esta configurado como Favorecimento MPEs"

        def analisa_item_cota_reservada_processo_configuracao_exclusividade():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_item_cota_reservada_processo_configuracao_exclusividade(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    select 
                        ip.i_entidades,
                        ip.i_ano_proc,
                        ip.i_processo,
                        ip.i_item,
                        ip.so_mpes_art48_lcf123_06, --soMPEs
                        p.favorec_me_epp
                    from compras.itens_processo ip
                    join compras.processos p on p.i_ano_proc = ip.i_ano_proc and p.i_processo = ip.i_processo and p.i_entidades = ip.i_entidades 
                    where p.favorec_me_epp = 1
                    and ip.tipo_cota = 'R'
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado o processo {row['i_processo']}/{row['i_ano_proc']} campo tipo cota para L")
                        comandoUpdate += f"""UPDATE compras.itens_processo set tipo_cota = 'L' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_item = {row['i_item']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_item_cota_reservada_processo_configuracao_exclusividade: {e}")
            return

        if item_cota_reservada_processo_configuracao_exclusividade:
            dado = analisa_item_cota_reservada_processo_configuracao_exclusividade()

            if corrigirErros and len(dado) > 0:
                corrige_item_cota_reservada_processo_configuracao_exclusividade(listDados=dado)

    if dadosList:
        analisa_corrige_item_cota_reservada_processo_configuracao_exclusividade(pre_validacao="item_cota_reservada_processo_configuracao_exclusividade")
