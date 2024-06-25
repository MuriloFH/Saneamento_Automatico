from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def processoAdmItemLivre(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       processo_so_mpes_sem_configuracao_favorecimento_mpes=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_processo_so_mpes_sem_configuracao_favorecimento_mpes(pre_validacao):
        nomeValidacao = "Item do processo configurado com o campo Só MPEs no processo, mais o processo não esta configurado como Favorecimento MPEs"

        def analisa_processo_so_mpes_sem_configuracao_favorecimento_mpes():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_so_mpes_sem_configuracao_favorecimento_mpes(listDados):
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
                        p.favorec_me_epp,
                        CASE 
                            when p.favorec_me_epp = 0 then 'N'
                        END	as novo_soMPEs
                    from compras.itens_processo ip
                    join compras.processos p on p.i_ano_proc = ip.i_ano_proc and p.i_processo = ip.i_processo and p.i_entidades = ip.i_entidades 
                    where ip.so_mpes_art48_lcf123_06 = 'S'
                    and p.favorec_me_epp = 0
                    and ip.tipo_cota = 'L'
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado o processo {row['i_processo']}/{row['i_ano_proc']} item {row['i_item']} campo Só MPEs para {row['novo_soMPEs']}")
                        comandoUpdate += f"""UPDATE compras.itens_processo set so_mpes_art48_lcf123_06 = '{row['novo_soMPEs']}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_item = {row['i_item']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_so_mpes_sem_configuracao_favorecimento_mpes: {e}")
            return

        if processo_so_mpes_sem_configuracao_favorecimento_mpes:
            dado = analisa_processo_so_mpes_sem_configuracao_favorecimento_mpes()

            if corrigirErros and len(dado) > 0:
                corrige_processo_so_mpes_sem_configuracao_favorecimento_mpes(listDados=dado)

    if dadosList:
        analisa_corrige_processo_so_mpes_sem_configuracao_favorecimento_mpes(pre_validacao="processo_so_mpes_sem_configuracao_favorecimento_mpes")
