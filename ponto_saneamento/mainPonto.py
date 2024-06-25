import datetime
from utilitarios.bancoDeAlteracoes.logAlteracoes import LogAlteracoes
from utilitarios.logExecucao.funcoesLogExecucao import LogTxt
from utilitarios.popularTabelaControle.popularTabelaControle import populaTabela
from validacoes.horarioPonto import validaHorarioPonto
from validacoes.apuracoes import validaApuracoes
from validacoes.ocorrenciaPonto import validaOcorrenciaPonto
from validacoes.permutas import validaPermutas

logTxt = LogTxt()

nomeOdbc = ''

# ------------Inicia Banco de logs------------
logAlteracoes = LogAlteracoes(host="localhost",
                              dataBase="logsAlteracoes",
                              usuario="postgres",
                              senha="postgres")

logAlteracoes.criaTabela()
# --------------------------------------------
populaTabela(odbc=nomeOdbc, sistema='Ponto')

timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")

validaHorarioPonto.horarioPonto(tipo_registro='1-Horario ponto', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                descricao_repetida=True)

validaHorarioPonto.horarioPonto(tipo_registro='2-Turma', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                descricao_repetida_turmas=True)

validaHorarioPonto.horarioPonto(tipo_registro='3-Alteracao Ponto', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                descricao_motivo_altera_ponto_maior_30_caracter=True)

validaApuracoes.apuracoes(tipo_registro='4-Marcacoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                          corrigirErros=True,
                          origem_marcacao_invalida=True)

validaOcorrenciaPonto.ocorrenciaPonto(tipo_registro='5-Ocorrencias Ponto', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                      corrigirErros=True,
                                      descricao_duplicada=True)

validaPermutas.permutas(tipo_registro='6-permuta', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                      corrigirErros=True,
                                      dt_inicio_e_fim_nula=True)

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
