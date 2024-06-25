import datetime
from utilitarios.bancoDeAlteracoes.logAlteracoes import LogAlteracoes
from utilitarios.logExecucao.funcoesLogExecucao import LogTxt
from utilitarios.popularTabelaControle.popularTabelaControle import populaTabela
from validacoes.logradouro import validaLogradouro
from validacoes.pessoas import validaPessoas
from validacoes.camposAdicionais import validaCamposAdicionais
from validacoes.pessoasFisicas import validaPessoasFisicas
from validacoes.dependente import validaDependente
from validacoes.pessoasJuridicas import validaPessoasJuridicas
from validacoes.bairro import validaBairro
from validacoes.tipoBase import validaTipoBase
from validacoes.atos import validaAtos
from validacoes.cargos import validaCargos
from validacoes.vinculoEmpregaticio import validaVinculoEmpregaticio
from validacoes.motivoRescisao import validaMotivoRescisao
from validacoes.folha import validaFolha
from validacoes.ferias import validaFerias
from validacoes.motivoAposentadoria import validaMotivoAposentadoria
from validacoes.historicoSalarial import validaHistoricoSalarial
from validacoes.variaveis import validaVariaveis
from validacoes.movimentacaoPessoal import validaMovimentacaoPessoal
from validacoes.afastamentos import validaAfastamentos
from validacoes.historicoFuncionario import validaHistoricoFuncionario
from validacoes.funcionarios import validaFuncionarios
from validacoes.organograma import validaOrganograma
from validacoes.baseCalculo import validaBaseCalculo
from validacoes.endereco import validaEndereco
from validacoes.rua import validaRua
from validacoes.mediaVantagem import validaMediaVantagem
from validacoes.rescisao import validaRescisao
from validacoes.grupoFuncional import validaGrupoFuncional
from validacoes.beneficio import validaBeneficio
from validacoes.lotacaoFisica import validaLotacaoFisica
from validacoes.nivelSalarial import validaNivelSalarial
from validacoes.caracteristica import validaCaracteristica
from validacoes.historicoTipoAdm import validaHistoricoTipoAdm
from validacoes.motivoAlteracaoSalarial import validaMotivoAlteracaoSalarial
from validacoes.localTrabalho import validaLocalTrabalho
from validacoes.estagiarios import validaEstagiarios
from validacoes.concursoProcessoSeletivo import validaConcursoProcessoSeletivo
from validacoes.localAvaliacao import validaLocalAvaliacao
from validacoes.configuracaoRais import validaConfiguracaoRais
from validacoes.configuracaoRaisCampo import validaConfiguracaoRaisCampo
from validacoes.agrupadorEvento import validaAgrupadorEvento
from validacoes.historicoPessoaFisica import validaHistoricoPessoaFisica
from validacoes.entidades import validaEntidades
from validacoes.dataFechamentoFolha import validaDataFechamentoFolha
from validacoes.beneficiario import validaBeneficiario
from validacoes.basesOutrasEmpresas import validaBasesOutrasEmpresas
from validacoes.periodoAquisitivoFerias import validaPeriodoAquisitivoFerias
from validacoes.processoTrabalhista import validaProcessoTrabalhista
from validacoes.aposentado import validaAposentado
from validacoes.emprestimo import validaEmprestimo
from validacoes.pensionista import validaPensionista
from validacoes.banco import validaBanco
from validacoes.dataContrato import validaDataContrato
from validacoes.evento import validaEvento
from validacoes.faltas import validaFaltas
from validacoes.licencaPremio import validaLicencaPremio
from validacoes.lancamentoEvento import validaLancamentoEvento

logTxt = LogTxt()

nomeOdbc = ''

# ------------Inicia Banco de logs------------
logAlteracoes = LogAlteracoes(host="localhost",
                              dataBase="logsAlteracoes",
                              usuario="postgres",
                              senha="postgres")

logAlteracoes.criaTabela()
# ------------Popula a tabela de controle-----
populaTabela(odbc=nomeOdbc, sistema='Folha')
# --------------------------------------------
timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
# ------------Inicia Pré-validações------------
validaLogradouro.logradouro(tipo_registro='01-logradouro', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            duplicidade_logradouro=True
                            )

validaPessoas.pessoas(tipo_registro='02-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      data_nascimento_maior_data_admissao=True
                      )

validaPessoas.pessoas(tipo_registro='03-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      data_vencimento_cnh_menor_data_emissao_1_habilitacao=True
                      )

validaCamposAdicionais.camposAdicionais(tipo_registro='04-Campos-Adicionais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        duplicidade_descricao_caracteristica=True
                                        )

validaPessoas.pessoas(tipo_registro='06-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      data_nascimento_maior_data_dependencia=True
                      )

validaDependente.dependente(tipo_registro='07-Dependente', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_data_nascimento_menor_data_nascimento_responsavel=True
                            )

validaPessoasFisicas.pessoasFisicas(tipo_registro='08-Pessoas-Fisicas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    cpf_duplicado=True
                                    )

validaPessoasFisicas.pessoasFisicas(tipo_registro='09-Pessoas-Fisicas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    pis_duplicado=True
                                    )

validaPessoasFisicas.pessoasFisicas(tipo_registro='10-Pessoas-Fisicas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    pis_invalido=True
                                    )

validaPessoasJuridicas.pessoasJuridicas(tipo_registro='11-Pessoas-Juridicas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        cnpj_nulo=True
                                        )

validaLogradouro.logradouro(tipo_registro='12-Logradouros', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            logradouro_caracter_especial_inicio_descricao=True
                            )

validaBairro.bairro(tipo_registro='13-Bairros', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    descricao_duplicada=True)

validaTipoBase.tipoBase(tipo_registro='14-Tipos-Bases', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                        corrigirErros=True,
                        descricao_duplicada=True)

validaLogradouro.logradouro(tipo_registro='15-Logradouros', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            logradouro_sem_cidade=True
                            )

validaAtos.atos(tipo_registro='16-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                numero_nulo=True
                )

validaAtos.atos(tipo_registro='17-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                duplicidade_numero_tipo_ato=True
                )

validaCargos.cargos(tipo_registro='18-Cargos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    cbo_nulo=True)

validaVinculoEmpregaticio.vinculoEmpregaticio(tipo_registro='19-Vínculo-empregaticio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              categoria_esocial_nulo=True)

validaVinculoEmpregaticio.vinculoEmpregaticio(tipo_registro='20-Vinculos-empregaticios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              duplicidade_vinculo_empregaticio=True)

validaMotivoRescisao.motivoRescisao(tipo_registro='21-Motivo-rescisao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    motivo_rescisao_sem_categoria_esocial=True)

validaFolha.folha(tipo_registro='22-Folha', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                  corrigirErros=True,
                  folha_sem_fechamento=True)

validaFerias.ferias(tipo_registro='23-Ferias', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    folha_ferias_sem_data_pagamento=True)

validaMotivoAposentadoria.motivoAposentadoria(tipo_registro='24-Motivo-aposentadoria', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              motivo_aposentadoria_sem_categoria_esocial=True)

validaHistoricoSalarial.historicoSalarial(tipo_registro='25-Historicos-salariais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                          corrigirErros=True,
                                          historico_salarial_com_salario_zerado=True)

"""Metodo considera_conselheiro setado automaticamente como False, caso alterar para True o sql irá considerar todos os funcionários independente se é ou não conselheiro"""
validaVariaveis.variavel(tipo_registro='26-Variaveis', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                         corrigirErros=True,
                         variaveis_dt_inicial_ou_final_maior_dt_rescisao=True,
                         considera_conselheiro=False,
                         )

validaMovimentacaoPessoal.movimentacaoPessoal(tipo_registro='27-Movimentações-pessoal', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              movimentacao_pessoal_duplicada=True)

validaAfastamentos.afastamentos(tipo_registro='28-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                afastamentos_duplicados=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro='29-Historicos-funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                alteracao_historico_funcionario_maior_data_rescisao=True)

validaFuncionarios.funcionarios(tipo_registro='30-Funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                alteracao_salario_funcionario_maior_data_rescisao=True)

validaFuncionarios.funcionarios(tipo_registro='31-Funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                alteracao_cargo_funcionario_maior_data_rescisao=True)

validaAfastamentos.afastamentos(tipo_registro='32-Afastamento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                tipo_afastamento_com_classificacao_invalida=True)

validaAtos.atos(tipo_registro='33-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                duplicidade_descricao_tipo_ato=True)

validaOrganograma.organograma(tipo_registro='34-Niveis-organogramas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              niveis_organograma_separador_invalido=True)

validaAtos.atos(tipo_registro='35-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                natureza_texto_juridico_nula=True)

validaAtos.atos(tipo_registro='36-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                data_fonte_divulgacao_menor_data_publicacao_ato=True)

validaFerias.ferias(tipo_registro='37-Ferias', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    cancelamento_ferias_sem_tipo_afastamento=True)

validaOrganograma.organograma(tipo_registro='38-Config-Organograma', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              config_organograma_descricao_maior_30_caracteres=True)

validaOrganograma.organograma(tipo_registro='39-Config-Organograma', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              config_organograma_descricao_duplicada=True)

validaPessoasFisicas.pessoasFisicas(tipo_registro='40-Pessoas-Fisicas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    rg_duplicado=True
                                    )

validaCargos.cargos(tipo_registro='41-Cargos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    cargo_descricao_duplicada=True)

validaBaseCalculo.baseCalculo(tipo_registro='42-Base-Calculo', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              bases_calc_outras_empresas_vigencia_invalida=True)

validaEndereco.endereco(tipo_registro='43-Endereço', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                        corrigirErros=True,
                        endereco_sem_numero=True)

validaRua.rua(tipo_registro='44-Rua', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
              corrigirErros=True,
              rua_sem_descricao=True)

validaFuncionarios.funcionarios(tipo_registro='45-Funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_sem_previdencia=True)

validaMediaVantagem.mediaVantagem(tipo_registro='46-Media-vantagem', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  evento_media_vantagem_sem_composicao=True)

validaMediaVantagem.mediaVantagem(tipo_registro='47-Media-vantagem', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  evento_media_vantagem_composicao_invalida=True)

validaFuncionarios.funcionarios(tipo_registro='48-Funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                data_adm_matricula_posterior_data_inicio_matricula_lotacao_fisica=True)

validaAfastamentos.afastamentos(tipo_registro='49-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                afastamento_motivo_maximo_150_caracteres=True)

validaFerias.ferias(tipo_registro='50-Ferias', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    data_inicial_afastamento_maior_data_final=True)

validaRescisao.rescisao(tipo_registro='51-Rescisoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                        corrigirErros=True,
                        rescisoes_aposentadoria_motivo_nulo=True)

validaGrupoFuncional.grupoFuncional(tipo_registro='52-Grupos-funcionais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    grupos_funcionais_duplicados=True)

validaBeneficio.beneficio(tipo_registro='53-Beneficio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                          corrigirErros=True,
                          func_planos_saude_vigencia_inicial_menor_vigencia_inicial_titular=True)

validaLotacaoFisica.lotacaoFisica(tipo_registro='54-Lotacao-fisica', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  numero_telefone_maior_9_caracteres=True)

validaAtos.atos(tipo_registro='55-Atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                ato_data_inicial_nulo=True)

validaNivelSalarial.nivelSalarial(tipo_registro='56-Niveis-salariais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  niveis_salariais_com_descricao_duplicada=True)

validaFuncionarios.funcionarios(tipo_registro='57-Funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_data_nomeacao_maior_data_posse=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro='58-Historico-funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                funcionario_conta_bancaria_invalida=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro='59-Historicos-funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                funcionario_com_mais_de_uma_previdencia=True)

validaAfastamentos.afastamentos(tipo_registro='60-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                afastamentos_dt_afastamento_menor_dt_admissao=True)

validaDependente.dependente(tipo_registro="61-Dependentess", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_sem_motivo_de_termino=True)

validaCargos.cargos(tipo_registro="62-Cargos", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    cargo_sem_configuracao_de_ferias=True)

validaCargos.cargos(tipo_registro="63-Cargos", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    quantidade_vaga_maior_9999=True)

validaCaracteristica.caracteristica(tipo_registro="64-Caracteristicas", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    observacao_maior_150_caracteres=True)

validaHistoricoTipoAdm.historicoTipoAdm(tipo_registro="65-Historico-tipo-adm", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        tipo_adm_com_teto_remuneratorio_nulo=True)

validaHistoricoSalarial.historicoSalarial(tipo_registro="66-Historicos_salariais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                          corrigirErros=True,
                                          historico_com_horas_mes_zerada=True)

validaFerias.ferias(tipo_registro="67-Ferias", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    ferias_dt_gozo_ini_menor_dt_admissao=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro="68-Historico-funcionarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                forma_pagamento_credito_sem_conta_vinculada=True)

validaMotivoAlteracaoSalarial.motivoAlteracaoSalarial(tipo_registro="69-Motivo-alteração-salarial", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                      corrigirErros=True,
                                                      descricao_duplicada=True)

validaFuncionarios.funcionarios(tipo_registro="70-Funcionarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                ferias_dt_gozo_fin_maior_rescisao=True)

"""Pré validação 71 foi desconsiderada por ser igual a 68"""

validaLocalTrabalho.localTrabalho(tipo_registro="72-Local-de-Trabalho", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  data_fim_menor_data_inicio=True)

validaDependente.dependente(tipo_registro="73-Dependente", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_data_nascimento_maior_data_nascimento_responsavel=True)

validaEstagiarios.estagiarios(tipo_registro="75-Estagiarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              estagiario_sem_numero_apolice_informado=True)

validaEstagiarios.estagiarios(tipo_registro="76-Estagiarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              estagiario_agente_integracao_nulo=True)

validaEstagiarios.estagiarios(tipo_registro="77-Estagiarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              estagiario_sem_responsavel=True)

validaAfastamentos.afastamentos(tipo_registro="78-Afastamentos", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                tipos_afast_sem_tipo_movto_pessoal=True)

validaFuncionarios.funcionarios(tipo_registro="79-Funcionarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                ferias_gozo_final_apos_rescisao=True)

validaFerias.ferias(tipo_registro="80-Ferias", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    motivo_cancelamento_maior_50_caracteres=True)

validaPessoasJuridicas.pessoasJuridicas(tipo_registro="81-Pessoa-juridica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        inscricao_municipal_superior_9_caracteres=True)

validaPessoasJuridicas.pessoasJuridicas(tipo_registro="82-Pessoa-juridica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        cnpj_invalido=True)

validaPessoasJuridicas.pessoasJuridicas(tipo_registro="83-Pessoa-juridica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        cnpj_duplicado=True)

validaPessoasFisicas.pessoasFisicas(tipo_registro="84-Pessoa-Fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    data_nascimento_nulo=True)

validaPessoasFisicas.pessoasFisicas(tipo_registro="85-Pessoa-Fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    filiacao_duplicada=True)

validaPessoasFisicas.pessoasFisicas(tipo_registro="86-Pessoa-Fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    email_invalido=True)

validaPessoasFisicas.pessoasFisicas(tipo_registro="87-Pessoa-Fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    dependente_mesma_pessoa_responsavel=True)

validaDependente.dependente(tipo_registro="88-Dependente", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_data_inicio_menor_data_nascimento=True)

validaDependente.dependente(tipo_registro="89-Dependente", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_dt_casamento_menor_dt_nascimento=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro='90-Historico-funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                codigo_esocial_duplicado=True)

validaFuncionarios.funcionarios(tipo_registro="91-Funcionarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                multiplos_locais_trabalho_principal=True)

validaFuncionarios.funcionarios(tipo_registro="92-Funcionarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_sem_local_trabalho_principal=True)

validaAposentado.aposentado(tipo_registro="93-APOSENTADO", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dt_hist_posterior_dt_cessacao=True)

validaHistoricoFuncionario.historicoFuncionario(tipo_registro='94-Historico-Matricula', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                hist_clt_sem_opcao_federal=True)

validaEmprestimo.emprestimo(tipo_registro='95-emprestimo', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dt_inicio_emprestimo_menor_dt_admissao=True)

validaPensionista.pensionista(tipo_registro='96-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              pensionista_menor_18_ano_sem_responsavel=True)

validaPessoas.pessoas(tipo_registro='97-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      num_certidao_maior_32_caracter=True)

validaLotacaoFisica.lotacaoFisica(tipo_registro='98-Lotacao-fisica', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  numero_telefone_maior_9_caracteres=True)

validaOrganograma.organograma(tipo_registro='99-Configuracao-Organograma', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              config_organograma_sem_nivel=True)

validaEstagiarios.estagiarios(tipo_registro="100-Estagiarios", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              estagiario_nao_presente_na_tabela_estagios=True)

validaBanco.banco(tipo_registro="101-banco", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                  corrigirErros=True,
                  banco_fora_padrao=True)

validaLogradouro.logradouro(tipo_registro='102-Logradouros', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            logradouro_sem_rua=True)

validaDataContrato.dataContrato(tipo_registro='103-datadecontrato', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                pensionista_dt_admissao_menor_dt_rescisao_instituidor=True)

validaEvento.eventos(tipo_registro='104-Eventos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                     corrigirErros=True,
                     evento_sem_historico=True)

validaAfastamentos.afastamentos(tipo_registro='105-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                falta_tipo_1_com_competencia=True)

# validação 106 igual a 97

validaFuncionarios.funcionarios(tipo_registro='107-Funcionario', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                dt_fim_lotacao_maior_dt_fim_contrato=True)

validaPensionista.pensionista(tipo_registro='108-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              tipo_pensao_nulo=True)

validaPessoas.pessoas(tipo_registro='109-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      dt_nascimento_maior_dt_chegada=True)

validaAfastamentos.afastamentos(tipo_registro='110-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                dt_afastamento_concomitante=True)

validaFerias.ferias(tipo_registro='111-Ferias', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    ferias_concomitantes=True)

validaAfastamentos.afastamentos(tipo_registro='112-Afastamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                dt_afastamento_concomitante_dt_ferias=True)

validaFaltas.faltas(tipo_registro='113-Faltas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    falta_concomitante_dt_afastamento=True)

validaFaltas.faltas(tipo_registro='114-Faltas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    falta_concomitante_dt_ferias=True)

validaFuncionarios.funcionarios(tipo_registro='115-Funcionario', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_sem_historico=True)

validaFuncionarios.funcionarios(tipo_registro='116-Funcionario', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_sem_historico_cargo=True)

validaFuncionarios.funcionarios(tipo_registro='117-Funcionario', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                funcionario_sem_historico_salarial=True)

validaLicencaPremio.licencaPremio(tipo_registro='118-Licenca-premio', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  faixa_maior_99=True)

validaLancamentoEvento.lancamentoEvento(tipo_registro='119-lancamento-evento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        lancamento_faltante=True)

validaConcursoProcessoSeletivo.concursoProcessoSeletivo(tipo_registro="120-concurso-processo-seletivo", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                        corrigirErros=True,
                                                        dt_homologacao_nulo=True)

validaLocalAvaliacao.localAvaliacao(tipo_registro="121-local-avaliacao", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    numero_sala_nulo=True)

validaConfiguracaoRais.configuracaoRais(tipo_registro="122-configuracao-rais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        controle_ponto_nulo=True)

validaConfiguracaoRais.configuracaoRais(tipo_registro="123-configuracao-rais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        inscricao_nulo_ou_CEI=True)

validaConfiguracaoRais.configuracaoRais(tipo_registro="124-configuracao-rais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        mes_base_nulo=True)

validaConfiguracaoRais.configuracaoRais(tipo_registro="125-configuracao-rais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        responsavel_nulo=True)

validaConfiguracaoRais.configuracaoRais(tipo_registro="126-configuracao-rais", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        contato_nulo=True)

validaConfiguracaoRaisCampo.configuracaoRaisCampo(tipo_registro="127-configuracao-rais-campo", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                  corrigirErros=True,
                                                  cnpj_nulo=True)

validaPessoas.pessoas(tipo_registro='128-Pessoas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                      corrigirErros=True,
                      pf_sem_historico=True)

validaMovimentacaoPessoal.movimentacaoPessoal(tipo_registro='129-movimentacao-pessoal', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              dt_vigorar_ato_maior_dt_movimentacao=True)

validaAgrupadorEvento.configuracaoRaisCampo(tipo_registro="130-agrupador-evento", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                            corrigirErros=True,
                                            ordenacao_nulo=True)

validaHistoricoPessoaFisica.historicoPessoaFisica(tipo_registro="131-historico-pessoa-fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                  corrigirErros=True,
                                                  dt_alteracao_menor_dt_nascimento=True)

validaEntidades.entidade(tipo_registro="132-entidades", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                         corrigirErros=True,
                         indicativo_educacional_nulo=True)

validaDataFechamentoFolha.dataFechamentoFolha(tipo_registro="133-dataFechamentoFolha", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              dt_fechamento_nulo=True)


validaLancamentoEvento.lancamentoEvento(tipo_registro='134-lancamento-evento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        lancamento_posterior_rescisao=True)

validaLancamentoEvento.lancamentoEvento(tipo_registro='135-lancamento-evento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        lancamento_posterior_cessacao=True)

validaAposentado.aposentado(tipo_registro='136-aposentado', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            aposentado_sem_rescisao=True)

validaBeneficiario.beneficiario(tipo_registro="137-beneficiario", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                beneficiario_nao_cadastrano_na_tabela=True)

validaBasesOutrasEmpresas.basesOutrasEmpresas(tipo_registro="138-base-outras-empresas", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              dt_fim_divergente=True)

validaPeriodoAquisitivoFerias.periodoAquisitivoFerias(tipo_registro="139-periodo-aquisitivo-ferias", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                      corrigirErros=True,
                                                      periodo_aquisitivo_ferias_concomitante=True)

validaProcessoTrabalhista.processoTrabalhista(tipo_registro="141-processo-trabalhista", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              dt_homologacao_invalida=True)

# validação 144 igual a 84

validaHistoricoPessoaFisica.historicoPessoaFisica(tipo_registro="145-historico-pessoa-fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                  corrigirErros=True,
                                                  dt_nascimento_nulo=True)

validaHistoricoPessoaFisica.historicoPessoaFisica(tipo_registro="146-historico-pessoa-fisica", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                  corrigirErros=True,
                                                  cpf_invalido=True)

validaPensionista.pensionista(tipo_registro='149-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              pensionista_nao_cadastrado_na_tabela=True)

validaPensionista.pensionista(tipo_registro='150-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              pensionista_sem_cessacao=True)

validaPensionista.pensionista(tipo_registro='151-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              pensionista_sem_dependente=True)

validaPensionista.pensionista(tipo_registro='152-pensionista', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              instituidor_sem_rescisao_por_aposentadoria=True)

# validação 153 igual a 91

validaDependente.dependente(tipo_registro="154-Dependentes", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_sem_dt_inicial=True)

validaDependente.dependente(tipo_registro="155-Dependentes", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_motivo_inicial_nulo=True)

validaDependente.dependente(tipo_registro="156-Dependentes", nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                            corrigirErros=True,
                            dependente_com_mais_de_uma_config_IRRF=True)

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
