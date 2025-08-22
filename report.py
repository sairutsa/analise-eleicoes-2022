import pandas as pd
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

# --- Configuration Constants ---
OUTPUT_FILENAME = "Analises_Urnas.pdf"
FIRST_PAGE_PATH = 'documents/first_page.pdf'
LAST_PAGE_PATH = 'documents/last_page.pdf'

# --- DataFrame Column Name Constants ---
PERC_LULA_COL = 'PERC_LULA'
IS_NEW_MACHINE_COL = 'SE_UE2020'
LULA_VOTES_COL = 'QT_LULA_2T'
BOLSONARO_VOTES_COL = 'QT_BOLSO_2T'
VALID_VOTES_COL = 'QT_VAL_PRESI_2T'
UF_COL = 'UF'

# --- Plotting Constants ---
PALETTE = {True: 'blue', False: 'orange'}
LEGEND_LABELS = ['Novas', 'Antigas']


def load_and_prepare_data():
    """
    Loads vote and machine model data from crawler modules, merges them,
    and calculates the percentage of votes for Lula.

    Returns:
        pd.DataFrame: A fully merged and prepared DataFrame for analysis.
    """
    print("Loading and merging data...")
    votos_df = votos_crawler.LoadVotosDataFrame()
    modelo_urnas_df = urna_log_crawler.LoadModeloUrnasDataFrame()
    
    # Merge the two dataframes on the polling section ID
    df = votos_df.merge(modelo_urnas_df, on='ID_SECAO', how='left')

    # Calculate the percentage of votes for Lula out of the total valid votes
    df[PERC_LULA_COL] = (df[LULA_VOTES_COL] / df[VALID_VOTES_COL]) * 100
    
    print("Data loaded successfully.")
    return df


def get_resized_image(image_buffer, target_width):
    """
    Resizes an image from a buffer to a target width while maintaining aspect ratio.

    Args:
        image_buffer (io.BytesIO): The buffer containing the image data.
        target_width (float): The desired width for the image in the PDF.

    Returns:
        reportlab.platypus.Image: A ReportLab Image object, ready to be added to the story.
    """
    image_buffer.seek(0)
    temp_img = PILImage.open(image_buffer)
    img_width, img_height = temp_img.size

    # Calculate the new height to maintain the aspect ratio
    ratio = target_width / img_width
    new_height = img_height * ratio
    
    image_buffer.seek(0) # Reset buffer pointer for ReportLab
    return Image(image_buffer, width=target_width, height=new_height)


def create_uf_comparison_plot(df_uf, uf_code, doc_width):
    """
    Generates a figure with two plots for a given state (UF):
    1. A scatter plot of absolute votes (Lula vs. Bolsonaro).
    2. A histogram of the percentage of votes for Lula.

    Args:
        df_uf (pd.DataFrame): The DataFrame filtered for a specific state.
        uf_code (str): The two-letter code for the state (e.g., 'SP').
        doc_width (float): The available width in the PDF document for the image.

    Returns:
        reportlab.platypus.Image: A ReportLab Image object containing the plots.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # --- Plot 1: Scatterplot of absolute votes ---
    sns.scatterplot(
        x=LULA_VOTES_COL, y=BOLSONARO_VOTES_COL, hue=IS_NEW_MACHINE_COL,
        data=df_uf, ax=ax1, palette=PALETTE, alpha=0.7, s=8.0
    )
    ax1.set_title(f'Votos Absolutos (Lula vs. Bolsonaro) em {uf_code}')
    ax1.set_xlabel('Votos para Lula')
    ax1.set_ylabel('Votos para Bolsonaro')
    ax1.legend(title='Modelo de Urna', labels=LEGEND_LABELS)

    # --- Plot 2: Histogram of vote percentage for Lula ---
    sns.histplot(
        data=df_uf, x=PERC_LULA_COL, hue=IS_NEW_MACHINE_COL,
        multiple="layer", ax=ax2, palette=PALETTE, kde=True
    )
    ax2.set_title(f'Distribuição do % de Votos para Lula em {uf_code}')
    ax2.set_xlabel('Percentual de Votos para Lula')
    ax2.set_ylabel('Frequência')
    ax2.legend(title='Modelo de Urna', labels=LEGEND_LABELS)

    plt.tight_layout()

    # Save the combined plot to an in-memory buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300)
    plt.close(fig)
    
    return get_resized_image(buf, doc_width)


def create_summary_plot(ufs, diffs, doc_width):
    """
    Creates a bar chart summarizing the average vote difference per state.

    Args:
        ufs (list): A list of state codes.
        diffs (list): A list of the calculated vote differences for each state.
        doc_width (float): The available width in the PDF document for the image.

    Returns:
        reportlab.platypus.Image: A ReportLab Image object containing the summary plot.
    """
    df_diferencas = pd.DataFrame({'UF': ufs, 'Diferenca': diffs})
    
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x='UF', y='Diferenca', data=df_diferencas, ax=ax)

    # Add percentage labels on top of each bar
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height:.1f}%', (p.get_x() + p.get_width() / 2., height),
                    ha='center', va='bottom', xytext=(0, 5), textcoords='offset points')

    ax.set_title("Diferença Média de % de Votos para Lula (Urnas Antigas - Novas)")
    ax.set_xlabel('Unidade da Federação')
    ax.set_ylabel('Diferença Média de Votos (%)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save the plot to an in-memory buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300)
    plt.close(fig)

    return get_resized_image(buf, doc_width)


def build_pdf(story, output_filename, first_page_path, last_page_path):
    """
    Builds the main PDF content in memory and then merges it with
    a cover page and a final page.

    Args:
        story (list): A list of ReportLab flowables that make up the main content.
        output_filename (str): The path to save the final, merged PDF.
        first_page_path (str): The path to the cover page PDF.
        last_page_path (str): The path to the final page PDF.
    """
    print("Building PDF...")
    buffer_content = io.BytesIO()
    doc = SimpleDocTemplate(buffer_content, pagesize=A4)
    
    # Build the main content in the memory buffer
    doc.build(story)
    buffer_content.seek(0)

    # --- Merge PDFs ---
    merger = PdfWriter()

    # Add the cover page if it exists
    if os.path.exists(first_page_path):
        merger.append(first_page_path)
    else:
        print(f"Warning: Cover page not found at '{first_page_path}'")

    # Add the generated content from the buffer
    merger.append(PdfReader(buffer_content))

    # Add the final page if it exists
    if os.path.exists(last_page_path):
        merger.append(last_page_path)
    else:
        print(f"Warning: Last page not found at '{last_page_path}'")

    # Save the final merged PDF to a file
    with open(output_filename, "wb") as f_out:
        merger.write(f_out)
    merger.close()
    print(f"PDF '{output_filename}' generated successfully!")


def main():
    """
    Main function to orchestrate the data loading, analysis,
    and PDF report generation.
    """
    df = load_and_prepare_data()
    if df.empty:
        print("Dataframe is empty. Aborting report generation.")
        return

    # --- PDF Setup ---
    story = []
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='UF_Heading', fontSize=14, spaceAfter=12, alignment=TA_CENTER))
    styles.add(ParagraphStyle(name='UF_Text', fontSize=12, spaceAfter=6))
    doc_width = A4[0] - 2 * inch # Calculate available width for images

    # --- Add General Title and Description ---
    story.append(Paragraph("Análise de Votos por Modelo de Urna (2º Turno - Brasil)", styles['Heading1']))
    story.append(Spacer(1, 0.2 * inch))
    description_text = "Esta análise apresenta, para cada Unidade da Federação, dois gráficos: um scatterplot comparando os votos absolutos para Lula e Bolsonaro e um histograma da distribuição de votos para Lula, ambos segmentados por urnas antigas e novas."
    story.append(Paragraph(description_text, styles['Normal']))
    story.append(Spacer(1, 0.4 * inch))

    # --- Generate Report for Each State ---
    ufs = sorted(df[UF_COL].unique())
    vote_differences = []
    for uf in ufs:
        print(f"Processing {uf}...")
        df_uf = df[df[UF_COL] == uf]
        if df_uf.empty:
            continue

        # Add UF-specific title
        story.append(Paragraph(f"Análise para {ufbr.dict_uf[uf]['nome']}", styles['UF_Heading']))
        story.append(Spacer(1, 0.2 * inch))

        # Create and add the comparison plots for the current UF
        uf_plot_image = create_uf_comparison_plot(df_uf, uf, doc_width)
        story.append(uf_plot_image)
        story.append(Spacer(1, 0.2 * inch))

        # --- Calculate and add statistical summary for the UF ---
        df_novas_uf = df_uf[df_uf[IS_NEW_MACHINE_COL] == True]
        df_antigas_uf = df_uf[df_uf[IS_NEW_MACHINE_COL] == False]

        media_perc_lula_novas = df_novas_uf[PERC_LULA_COL].mean()
        media_perc_lula_antigas = df_antigas_uf[PERC_LULA_COL].mean()
        diferenca = media_perc_lula_antigas - media_perc_lula_novas
        vote_differences.append(diferenca)

        story.append(Paragraph(f"Total de seções: {df_uf.shape[0]}", styles['UF_Text']))
        story.append(Paragraph(f"Total de urnas antigas: {df_antigas_uf.shape[0]}", styles['UF_Text']))
        story.append(Paragraph(f"Total de urnas novas: {df_novas_uf.shape[0]}", styles['UF_Text']))
        story.append(Paragraph(f"Média do % votos para Lula nas Urnas novas: {media_perc_lula_novas:.1f}%", styles['UF_Text']))
        story.append(Paragraph(f"Média do % votos para Lula nas Urnas antigas: {media_perc_lula_antigas:.1f}%", styles['UF_Text']))
        story.append(Paragraph(f"Diferença: {diferenca:.1f}%", styles['UF_Text']))
        story.append(PageBreak())

    # --- Generate and Add Final Summary Page ---
    story.append(Paragraph("Diferença Média de % de Votos para Lula por UF", styles['Heading1']))
    story.append(Spacer(1, 0.2 * inch))
    summary_description = "O gráfico exibe, para cada UF, a diferença das médias dos percentuais obtidos pelo PT nas urnas antigas e novas. Números positivos significam vantagem nas antigas, números negativos significam vantagem nas novas."
    story.append(Paragraph(summary_description, styles['Normal']))
    story.append(Spacer(1, 0.4 * inch))

    summary_plot_image = create_summary_plot(ufs, vote_differences, doc_width)
    story.append(summary_plot_image)
    story.append(Spacer(1, 0.4 * inch))

    # --- Build the Final PDF ---
    build_pdf(story, OUTPUT_FILENAME, FIRST_PAGE_PATH, LAST_PAGE_PATH)


if __name__ == '__main__':
    main()