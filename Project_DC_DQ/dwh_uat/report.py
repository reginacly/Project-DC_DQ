import matplotlib.pyplot as plt
import pandas as pd

def generate_report_png(
    source_result,
    target_df,
    source_information_schema,
    target_information_schema,
    output_path="report_refined.png"
):
    # --- Data Preparation (Schema) ---
    source_schema_df = source_information_schema.select(["column_name", "data_type"]).to_pandas()
    target_schema_df = target_information_schema.select(["column_name", "data_type"]).to_pandas()

    # --- Data Preparation (Summary) ---
    source_summary = pd.DataFrame({
        "Metric": ["Record Count", "Column Count", "Missing Columns"],
        "Value": [source_result.record_count, source_result.column_count, "-"]
    })

    target_summary = pd.DataFrame({
        "Metric": ["Record Count", "Column Count", "Missing Columns"],
        "Value": [len(target_df), len(target_df.columns), "-"]
    })

    # --- Plot Setup ---
    # Menggunakan gridspec_kw untuk mengatur rasio tinggi antara Summary (kecil) dan Schema (panjang)
    fig, axs = plt.subplots(2, 2, figsize=(16, 14), gridspec_kw={'height_ratios': [1, 4]})
    fig.patch.set_facecolor('#f8f9fa') # Background abu-abu sangat muda (UI look)

    def draw_styled_table(ax, df, title, header_color='#40466e', row_colors=['#ffffff', '#f1f1f2']):
        ax.axis('off')
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20, color='#333333')

        # Create table
        table = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            loc='upper center',
            cellLoc='left'
        )

        # Styling
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2.2)  # Menambah tinggi baris agar teks tidak sesak

        for (row, col), cell in table.get_celld().items():
            cell.set_edgecolor('#dddddd') # Border lebih halus
            if row == 0:
                # Header Style
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor(header_color)
            else:
                # Zebra Striping
                cell.set_facecolor(row_colors[row % len(row_colors)])
            
            # Memberikan margin kiri pada teks (padding)
            cell.set_text_props(ha='left')
            cell.PAD = 0.1 

    # --- Drawing ---
    # Row 1: Summary
    draw_styled_table(axs[0, 0], source_summary, "SOURCE SUMMARY")
    draw_styled_table(axs[0, 1], target_summary, "TARGET SUMMARY")

    # Row 2: Schema
    draw_styled_table(axs[1, 0], source_schema_df, "SOURCE SCHEMA")
    draw_styled_table(axs[1, 1], target_schema_df, "TARGET SCHEMA")

    # Final Adjustments
    plt.tight_layout(pad=4.0)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"✅ Enhanced report saved: {output_path}")