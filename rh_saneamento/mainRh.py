import datetime
from utilitarios.bancoDeAlteracoes.logAlteracoes import LogAlteracoes
from utilitarios.logExecucao.funcoesLogExecucao import LogTxt
from utilitarios.popularTabelaControle.popularTabelaControle import populaTabela
from validacoes.avaliacoes import validaAvaliacoes
from validacoes.distancias import validaDistancia
from validacoes.licencaPremio import validaLicencaPremio
from validacoes.funcaoGratificada import validaFuncaoGratificada

logTxt = LogTxt()

nomeOdbc = ''

# ------------Inicia Banco de logs------------
logAlteracoes = LogAlteracoes(host="localhost",
                              dataBase="logsAlteracoes",
                              usuario="postgres",
                              senha="postgres")

logAlteracoes.criaTabela()
# --------------------------------------------
populaTabela(odbc=nomeOdbc, sistema='Rh')

timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")

validaAvaliacoes.avaliacoes(tipo_registro='01-local-avaliacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            local_avaliacao_bloco_nulo=True)

validaDistancia.distancia(tipo_registro='02-Distancia', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                          corrigirErros=True,
                          km_distancia_nulo=True)

validaLicencaPremio.licencaPremio(tipo_registro='03-Matricula-licenca-premio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  dt_admissao_maior_dt_inicio_licenca=True)

validaFuncaoGratificada.funcaoGratificada(tipo_registro='04-funcao-gratificada', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                          corrigirErros=True,
                                          dt_inicial_dt_final_menor_dt_admissao=True)

validaLicencaPremio.licencaPremio(tipo_registro='05-periodo-aquisitivo-licenca-premio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  dt_inicial_maior_dt_final_licenca=True)

validaLicencaPremio.licencaPremio(tipo_registro='06-periodo-aquisitivo-licenca-premio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  qtd_dias_direito_nulo=True)

validaLicencaPremio.licencaPremio(tipo_registro='07-periodo-aquisitivo-licenca-premio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  dt_inicial_e_final_licenca_nulo=True)

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
