using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenPlot.Ingestor.Gsf
{
    internal static class TerminalResolver
    {
        /// <summary>
        /// Resolve o terminal pelo idName (no XML isso corresponde a Terminal.Id em string).
        /// </summary>
        public static Terminal Resolve(SystemData sys, string idName)
        {
            if (string.IsNullOrWhiteSpace(idName))
                throw new Exception("terminal_id não informado.");

            var term = sys.Terminals.FirstOrDefault(t => t.Id.ToString() == idName);
            if (term == null)
                throw new Exception("Terminal não encontrado no XML: " + idName);

            return term;
        }

        /// <summary>
        /// Mapeia a lista de nomes de sinais para os Channels do terminal.
        /// É case-insensitive e aceita Name ou Id (string).
        /// Se 'signals' vier vazio/nulo, retorna todos os canais.
        /// </summary>
        /// 
        /*
        public static List<Channel> MapChannels(Terminal term, List<string> signals)
        {
            if (term == null) throw new ArgumentNullException(nameof(term));

            // Sem filtro → todos os canais
            if (signals == null || signals.Count == 0)
                return term.Channels.ToList();

            var set = new HashSet<string>(signals, StringComparer.OrdinalIgnoreCase);

            return term.Channels
                .Where(c => set.Contains(c.Name ?? c.Id.ToString()))
                .ToList();
        }
        */
    }
}
