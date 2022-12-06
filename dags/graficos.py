# Função para gerar os gráficos
def graficoBarras(df, var_x, var_y, nm_fig, consumer_path, title, xlabel, ylabel):
    import matplotlib.pyplot as plt
    from seaborn import barplot, color_palette
    plt.ioff()
    paleta_cores = color_palette("pastel") 
    plt.figure(figsize=(17,10))
    plot = barplot(data=df, 
                    x=df[var_x], 
                    y=df[var_y].round(), 
                    errorbar=('ci', False), 
                    palette=paleta_cores)
    for i in plot.patches:
        h = i.get_height()
        plot.annotate('{:0.0f}'.format(h), 
                    (i.get_x() + (i.get_width() / 2), h), 
                    ha='center', 
                    va='baseline', 
                    fontsize=14, 
                    xytext=(0, 2), textcoords='offset points'
                    )
    plt.title(title, fontsize=20)
    plt.xticks(rotation=45, ha='right')
    plt.xlabel(xlabel, fontsize=25)
    plt.ylabel(ylabel, fontsize=18)
    plt.savefig(consumer_path + nm_fig +'.pdf', format='pdf');
