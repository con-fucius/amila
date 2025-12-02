import { useState } from 'react';
import { useDatabaseType, useChatActions } from '@/stores/chatStore';

export function useChatUIState() {
  const [input, setInput] = useState('');
  const [showHistory, setShowHistory] = useState(false);
  const [reasoningOpen, setReasoningOpen] = useState<Record<string, boolean>>({});
  const [chartOpen, setChartOpen] = useState<Record<string, boolean>>({});
  const [showSuggestions, setShowSuggestions] = useState(false);
  
  // Use global database type from store
  const databaseType = useDatabaseType();
  const { setDatabaseType } = useChatActions();

  const toggleReasoning = (messageId: string) => {
    setReasoningOpen((prev) => ({
      ...prev,
      [messageId]: !prev[messageId],
    }));
  };

  const toggleChart = (messageId: string) => {
    setChartOpen((prev) => ({
      ...prev,
      [messageId]: !prev[messageId],
    }));
  };

  return {
    input, setInput,
    showHistory, setShowHistory,
    reasoningOpen, setReasoningOpen,
    chartOpen, setChartOpen,
    showSuggestions, setShowSuggestions,
    databaseType, setDatabaseType,
    toggleReasoning,
    toggleChart,
  };
}
