import pandas
import votos_crawler
import urna_log_crawler
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import inch
import io
from PIL import Image as PILImage # Importa a biblioteca Pillow
from pypdf import PdfWriter, PdfReader
import os
from pyUFbr.baseuf import ufbr


votos_df = votos_crawler.LoadVotosDataFrame()
modelo_urnas_df = urna_log_crawler.LoadModeloUrnasDataFrame()
df = votos_df.merge(modelo_urnas_df, on='ID_SECAO', how='left')

df['PERC_LULA'] = (df.QT_LULA_2T / df.QT_VAL_PRESI_2T) * 100

diff_lula_antigas_novas = []

if not df.empty:
    # Lista de UFs a serem analisadas
    ufs = ufbr.list_uf

    # Configura\u00e7\u00e3o para o PDF
    buffer_content = io.BytesIO()
    doc = SimpleDocTemplate(buffer_content, pagesize=A4)
    story = []
    styles = getSampleStyleSheet()

    # Adiciona estilos de par\u00e1grafo personalizados para o relat\u00f3rio
    styles.add(ParagraphStyle(name='UF_Heading', fontSize=14, spaceAfter=12, alignment=TA_CENTER))
    styles.add(ParagraphStyle(name='UF_Text', fontSize=12, spaceAfter=6))

    # Adiciona um t\u00edtulo geral ao documento
    story.append(Paragraph("An\u00e1lise de Votos por Modelo de Urna (2\u00ba Turno - Brasil)", styles['Heading1']))
    story.append(Spacer(1, 0.2 * inch))
    description_text = "Esta an\u00e1lise apresenta, para cada Unidade da Federa\u00e7\u00e3o, dois gr\u00e1ficos: um scatterplot comparando os votos absolutos para Lula e Bolsonaro e um histograma da distribui\u00e7\u00e3o de votos para Lula, ambos segmentados por urnas antigas e novas."
    story.append(Paragraph(description_text, styles['Normal']))
    story.append(Spacer(1, 0.4 * inch))

    for uf in ufs:
        # Filtra os dados pela UF atual
        df_uf = df[df.UF == uf]
        if df_uf.empty:
            continue

        # Adiciona o t\u00edtulo da se\u00e7\u00e3o para a UF
        story.append(Paragraph(f"An\u00e1lise para {ufbr.dict_uf[uf]['nome']}", styles['UF_Heading']))
        story.append(Spacer(1, 0.2 * inch))

        # Cria os dois gr\u00e1ficos
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))  # Ajuste no figsize para uma melhor propor\u00e7\u00e3o

        # Gr\u00e1fico 1: Scatterplot de votos absolutos
        sns.scatterplot(
            x='QT_LULA_2T',
            y='QT_BOLSO_2T',
            hue='SE_UE2020',
            data=df_uf,
            ax=ax1,
            palette={True: 'blue', False: 'orange'},
            alpha=0.7,
            s=8.0,
        )

        ax1.set_title(f'Votos Absolutos (Lula vs. Bolsonaro) em {uf}')
        ax1.set_xlabel('N\u00famero de Votos para Lula')
        ax1.set_ylabel('N\u00famero de Votos para Bolsonaro')
        ax1.legend(title='Modelo de Urna', labels=['Novas', 'Antigas'])

        # Gr\u00e1fico 2: Histograma do percentual de votos de Lula
        sns.histplot(
            data=df_uf,
            x='PERC_LULA',
            hue='SE_UE2020',
            multiple="layer",
            ax=ax2,
            palette={True: 'blue', False: 'orange'},
            kde=True
        )

        ax2.set_title(f'Distribui\u00e7\u00e3o do % de Votos para Lula em {uf}')
        ax2.set_xlabel('Percentual de Votos para Lula')
        ax2.set_ylabel('Frequ\u00eancia')
        ax2.legend(title='Modelo de Urna', labels=['Novas', 'Antigas'])

        plt.tight_layout()

        # Salva o gr\u00e1fico em um buffer de imagem
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=300)
        plt.close(fig)
        buf.seek(0)

        # Adiciona a imagem ao documento
        # Redimensiona a imagem para que se ajuste \u00e0 p\u00e1gina
        # Define a largura m\u00e1xima da imagem com base nas margens do documento
        max_width = doc.width
        max_height = doc.height

        # O problema estava aqui. Usamos o Pillow para obter as dimens\u00f5es corretas.
        temp_img = PILImage.open(buf)
        img_width, img_height = temp_img.size

        # Calcula a nova largura e altura para manter a propor\u00e7\u00e3o e caber na p\u00e1gina
        ratio = min(max_width / img_width, max_height / img_height)
        new_width = img_width * ratio
        new_height = img_height * ratio

        story.append(Image(buf, width=new_width, height=new_height))
        story.append(Spacer(1, 0.2 * inch))

        # Adiciona o texto de an\u00e1lise da UF
        total_secoes = df_uf.shape[0]
        total_antigas = df_uf[df_uf.SE_UE2020 == False].shape[0]
        total_novas = df_uf[df_uf.SE_UE2020 == True].shape[0]
        df_novas_uf = df_uf[df_uf.SE_UE2020 == True]
        df_antigas_uf = df_uf[df_uf.SE_UE2020 == False]

        media_perc_lula_novas = df_novas_uf.PERC_LULA.mean()
        media_perc_lula_antigas = df_antigas_uf.PERC_LULA.mean()
        diferenca = media_perc_lula_antigas - media_perc_lula_novas
        diff_lula_antigas_novas.append(diferenca)

        story.append(Paragraph(f"Total de se\u00e7\u00f5es: {total_secoes}", styles['UF_Text']))
        story.append(Paragraph(f"Total de urnas antigas: {total_antigas}", styles['UF_Text']))
        story.append(Paragraph(f"Total de urnas novas: {total_novas}", styles['UF_Text']))
        story.append(Paragraph(f"Média do % votos para Lula nas Urnas novas: {media_perc_lula_novas:.1f}%",
                               styles['UF_Text']))
        story.append(Paragraph(f"Média do % votos para Lula nas Urnas antigas: {media_perc_lula_antigas:.1f}%",
                               styles['UF_Text']))
        story.append(Paragraph(f"Diferença: {diferenca:.1f}%",
                               styles['UF_Text']))
        story.append(Spacer(1, 0.4 * inch))
        story.append(PageBreak())

    # --- Cria o gr\u00e1fico da \u00faltima p\u00e1gina ---
    story.append(Paragraph("Diferen\u00e7a M\u00e9dia de % de Votos para Lula por UF", styles['Heading1']))
    story.append(Spacer(1, 0.2 * inch))
    description_text = "O gráfico exibe, para cada UF, a diferença das médias dos percentuais obtidos pelo PT nas urnas antigas e novas. Números positivos significam vantagem nas antigas, números negativos significam vantagem nas novas."
    story.append(Paragraph(description_text, styles['Normal']))
    story.append(Spacer(1, 0.4 * inch))

    # Converte o dicion\u00e1rio em um dataframe para o seaborn
    data = {
        'UF' : ufs,
        'Diferenca' : diff_lula_antigas_novas
    }
    df_diferencas = pandas.DataFrame(data)

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(
        x='UF',
        y='Diferenca',
        data=df_diferencas,
        ax=ax,
        #palette='viridis'
    )

    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height:.1f}%',
                    (p.get_x() + p.get_width() / 2., height),
                    ha='center', va='bottom',
                    xytext=(0, 5), textcoords='offset points')

    ax.set_title("Diferen\u00e7a M\u00e9dia de % de Votos para Lula (Urnas Antigas - Novas)")
    ax.set_xlabel('Unidade da Federa\u00e7\u00e3o')
    ax.set_ylabel('Diferen\u00e7a M\u00e9dia de Votos (%)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Salva o gr\u00e1fico em um buffer
    buf_final = io.BytesIO()
    plt.savefig(buf_final, format='png', dpi=300)
    plt.close(fig)
    buf_final.seek(0)

    # Adiciona a imagem final ao documento
    max_width = doc.width
    temp_img_final = PILImage.open(buf_final)
    img_width, img_height = temp_img_final.size

    ratio = max_width / img_width
    new_width = img_width * ratio
    new_height = img_height * ratio

    story.append(Image(buf_final, width=new_width, height=new_height))
    story.append(Spacer(1, 0.4 * inch))

    # Constr\u00f3i o documento PDF principal no buffer
    doc.build(story)

    # --- Junta os PDFs ---
    merger = PdfWriter()
    output_filename = "Analises_Urnas.pdf"

    # Adiciona a capa
    first_page_path = 'documents/first_page.pdf'
    if os.path.exists(first_page_path):
        merger.append(first_page_path)
    else:
        print(f"Aviso: Arquivo da capa '{first_page_path}' n\u00e3o encontrado.")

    # Adiciona o conte\u00fado gerado
    buffer_content.seek(0)
    merger.append(PdfReader(buffer_content))

    # Adiciona a \u00faltima p\u00e1gina
    last_page_path = 'documents/last_page.pdf'
    if os.path.exists(last_page_path):
        merger.append(last_page_path)
    else:
        print(f"Aviso: Arquivo da \u00faltima p\u00e1gina '{last_page_path}' n\u00e3o encontrado.")

    # Salva o PDF final
    with open(output_filename, "wb") as f_out:
        merger.write(f_out)
    merger.close()
    print(f"PDF '{output_filename}' gerado com sucesso!")