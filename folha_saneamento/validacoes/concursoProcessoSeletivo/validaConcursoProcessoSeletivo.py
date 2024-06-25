from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import colorama


def concursoProcessoSeletivo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                             corrigirErros=False,
                             dt_homologacao_nulo=False
                             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_homologacao_nulo(pre_validacao):
        nomeValidacao = "Data de homologacao do concurso não informada"

        def analisa_dt_homologacao_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_homologacao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select concursos.i_entidades, 
                                            concursos.i_concursos,
                                            COUNT(i_candidatos) as qtdCandidatos 
                                            from bethadba.candidatos
                                            left join bethadba.concursos on (candidatos.i_concursos = concursos.i_concursos)
                                            where candidatos.dt_nomeacao is null
                                            and candidatos.dt_posse is null
                                            and candidatos.dt_doc_nao_posse is null
                                            and candidatos.dt_prorrog_posse is null
                                            and concursos.dt_homolog is null
                                            group by concursos.i_entidades, concursos.i_concursos
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadosConcurso = banco.consultar(f"""SELECT dt_validade, dt_prorrog, dt_encerra from bethadba.concursos c where c.i_entidades = {row['i_entidades']} and i_concursos = '{row['i_concursos']}'""")

                        if len(dadosConcurso) > 0:
                            dtValidade = dadosConcurso[0]['dt_validade']
                            dtProrrogacao = dadosConcurso[0]['dt_prorrog']
                            dtEncerramento = dadosConcurso[0]['dt_encerra']

                            if dtEncerramento:
                                newDtHomologacao = dtEncerramento
                            elif dtProrrogacao:
                                newDtHomologacao = dtProrrogacao
                            elif dtValidade:
                                newDtHomologacao = dtValidade
                            else:
                                print(colorama.Fore.RED, f"O concurso {row['i_concursos']} da entidade {row['i_entidades']} não possui nenhuma data válida, favor analisar manualmente.", colorama.Fore.RESET)
                                continue

                            comandoUpdate += f"""UPDATE bethadba.concursos set dt_homolog = '{newDtHomologacao}' where i_entidades = {row['i_entidades']} and i_concursos = '{row['i_concursos']}';"""
                        else:
                            print(colorama.Fore.RED, f"Não localizado os dados do concurso {row['i_concursos']} da entidade {row['i_entidades']}", colorama.Fore.RESET)
                            continue

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_homologacao_nulo: {e}")
            return

        if dt_homologacao_nulo:
            dado = analisa_dt_homologacao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_dt_homologacao_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_dt_homologacao_nulo(pre_validacao="dt_homologacao_nulo")
